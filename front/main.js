import Map from "ol/Map.js";
import View from "ol/View.js";
import { Draw, Modify, Select, Snap } from "ol/interaction.js";
import { OSM, Vector as VectorSource } from "ol/source.js";
import { Tile as TileLayer, Vector as VectorLayer } from "ol/layer.js";
import GeoJSON from "ol/format/GeoJSON.js";
import { bbox } from "ol/loadingstrategy";

const url = document.getElementById("url");

const raster = new TileLayer({
  source: new OSM(),
});

var vectorSource = new VectorSource({
  strategy: bbox,
  loader: function (extent, resolution, projection) {
    const proj = projection.getCode();
    fetch(`${url.value}/select?rect=${extent.join(",")}&proj=${proj}`, {
      cache: "no-cache",
    }).then(async function (response) {
      if (!response.ok) {
        console.log(response);
      }
      var t = await response.text();
      var features = new GeoJSON().readFeatures(t);
      vectorSource.addFeatures(features);
    });
  },
});

vectorSource.on("error", function (ev) {
  console.log("error", ev);
});

const vector = new VectorLayer({
  source: vectorSource,
});

let zoom = 12;
let pos = [3374339, 8388441];
let posStr = localStorage.getItem("center");
if (posStr != null) {
  pos = JSON.parse(posStr);
}
let zoomStr = localStorage.getItem("zoom");
if (zoomStr != null) {
  zoom = JSON.parse(zoomStr);
}

console.log(pos);

let view = new View({
  center: pos,
  zoom: zoom,
});
view.addEventListener("change", function (ev) {
  localStorage.setItem("center", JSON.stringify(view.getCenter()));
  localStorage.setItem("zoom", JSON.stringify(view.getZoom()));
});

const map = new Map({
  layers: [raster, vector],
  target: "map",
  view: view,
});

//localStorage.setItem("myCat", "Tom");

const optionsForm = document.getElementById("options-form");

const DrawEnd = function (ev) {
  console.log("drawfinished");

  ev.feature.setId(crypto.randomUUID());
  console.log(ev);

  var encoded = new GeoJSON().writeFeature(ev.feature);
  fetch(`${url.value}/insert`, {
    method: "POST",
    cache: "no-cache",
    headers: {
      "Content-Type": "application/json",
    },
    body: encoded,
  }).then(async function (response) {
    if (!response.ok) {
      console.log(response);
    }
  });
};

const ExampleDraw = {
  init: function () {
    map.addInteraction(this.Point);
    this.Point.setActive(false);
    this.Point.addEventListener("drawend", DrawEnd);
    map.addInteraction(this.LineString);
    this.LineString.setActive(false);
    this.LineString.addEventListener("drawend", DrawEnd);
    map.addInteraction(this.Polygon);
    this.Polygon.setActive(false);
    this.Polygon.addEventListener("drawend", DrawEnd);
  },
  Point: new Draw({
    source: vector.getSource(),
    type: "Point",
  }),
  LineString: new Draw({
    source: vector.getSource(),
    type: "LineString",
  }),
  Polygon: new Draw({
    source: vector.getSource(),
    type: "Polygon",
  }),
  activeDraw: null,
  setActive: function (active) {
    if (this.activeDraw) {
      this.activeDraw.setActive(false);
      this.activeDraw = null;
    }
    if (active) {
      const type = optionsForm.elements["draw-type"].value;
      this.activeDraw = this[type];
      this.activeDraw.setActive(true);
    }
  },
};
ExampleDraw.init();

const ModifyEnd = function (ev) {
  console.log("modifyend");
  console.log(ev);

  const features = ev.features.getArray();
  console.log(features);
  features.forEach(function (each) {
    if (each.getId() == null) {
      return;
    }

    var encoded = new GeoJSON().writeFeature(each);
    fetch(`${url.value}/replace`, {
      method: "POST",
      cache: "no-cache",
      headers: {
        "Content-Type": "application/json",
      },
      body: encoded,
    }).then(async function (response) {
      if (!response.ok) {
        console.log(response);
      }
    });
  });
};

const ExampleModify = {
  init: function () {
    this.select = new Select();
    map.addInteraction(this.select);

    this.modify = new Modify({
      features: this.select.getFeatures(),
    });
    this.modify.addEventListener("modifyend", ModifyEnd);
    map.addInteraction(this.modify);

    this.setEvents();
  },
  setEvents: function () {
    const selectedFeatures = this.select.getFeatures();

    this.select.on("change:active", function () {
      selectedFeatures.forEach(function (each) {
        selectedFeatures.remove(each);
      });
    });
  },
  setActive: function (active) {
    this.select.setActive(active);
    this.modify.setActive(active);
  },
};
ExampleModify.init();
/**
 * Let user change the geometry type.
 * @param {Event} e Change event.
 */
optionsForm.onchange = function (e) {
  const type = e.target.getAttribute("name");
  if (type == "draw-type") {
    ExampleModify.setActive(false);
    ExampleDraw.setActive(true);
    optionsForm.elements["interaction"].value = "draw";
  } else if (type == "interaction") {
    const interactionType = e.target.value;
    if (interactionType == "modify") {
      ExampleDraw.setActive(false);
      ExampleModify.setActive(true);
    } else if (interactionType == "draw") {
      ExampleDraw.setActive(true);
      ExampleModify.setActive(false);
    }
  } else if (type == "geojson") {
    console.log(vector);
  }
};

ExampleDraw.setActive(true);
ExampleModify.setActive(false);

// The snap interaction must be added after the Modify and Draw interactions
// in order for its map browser event handlers to be fired first. Its handlers
// are responsible of doing the snapping.
const snap = new Snap({
  source: vector.getSource(),
});
map.addInteraction(snap);

var deleteFeature = function (evt) {
  if (evt.code == "Backspace") {
    var selectCollection = ExampleModify.select.getFeatures();

    selectCollection.forEach(function (each) {
      vectorSource.removeFeature(each);

      if (each.getId() == null) {
        console.log("no id for feature");
        return;
      }

      let encoded = new GeoJSON().writeFeature(each);
      fetch(`${url.value}/delete`, {
        method: "POST",
        cache: "no-cache",
        headers: {
          "Content-Type": "application/json",
        },
        body: encoded,
      }).then(async function (response) {
        if (!response.ok) {
          console.log(response);
        }
      });
    });
  }
};
document.addEventListener("keydown", deleteFeature, false);

const okButton = document.getElementById("dump");
okButton.onclick = function (e) {
  var features = vectorSource.getFeatures();
  console.log(features);
};
