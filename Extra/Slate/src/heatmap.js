/**
 * SLATE CODE SANDBOX — JAVASCRIPT TAB
 * Sector Annual Return Heatmap
 */

var SECTOR_COLORS = {
  "Technology":               "#6366f1",
  "Communication Services":   "#8b5cf6",
  "Financial Services":       "#0ea5e9",
  "Consumer Cyclical":        "#f59e0b",
  "Healthcare":               "#10b981",
  "Industrials":              "#64748b",
  "Consumer Defensive":       "#f97316",
  "Energy":                   "#ef4444",
  "Utilities":                "#14b8a6",
  "Real Estate":              "#a855f7",
  "Basic Materials":          "#84cc16"
};

var LEGEND_ITEMS = [
  { label: "\u226540%",      bg: "#166534", text: "#fff"    },
  { label: "20\u201340%",    bg: "#15803d", text: "#fff"    },
  { label: "10\u201320%",    bg: "#16a34a", text: "#fff"    },
  { label: "0\u201310%",     bg: "#4ade80", text: "#052e16" },
  { label: "-10\u20130%",    bg: "#f87171", text: "#fff"    },
  { label: "-20\u2013-10%",  bg: "#dc2626", text: "#fff"    },
  { label: "< -20%",         bg: "#991b1b", text: "#fff"    }
];

function heatColor(val) {
  if (val === null || val === undefined) return "#1e293b";
  if (val >= 40)  return "#166534";
  if (val >= 20)  return "#15803d";
  if (val >= 10)  return "#16a34a";
  if (val >= 0)   return "#4ade80";
  if (val >= -10) return "#f87171";
  if (val >= -20) return "#dc2626";
  return "#991b1b";
}

function heatText(val) {
  if (val === null || val === undefined) return "#94a3b8";
  return Math.abs(val) >= 20 ? "#fff" : val >= 0 ? "#052e16" : "#fff";
}

function formatVal(val) {
  if (val === null || val === undefined) return "\u2013";
  return val >= 0 ? "+" + val.toFixed(1) : val.toFixed(1);
}

function loadData() {
  var data = {};

  try {
    var state = SlateFunctions.getState();
    if (!state || !state.queryRows) return data;

    var raw = state.queryRows;

    // Columnar format from Slate queries
    if (raw.sector_name && Array.isArray(raw.sector_name)) {
      var len = raw.sector_name.length;
      for (var i = 0; i < len; i++) {
        var sector = raw.sector_name[i];
        var year   = String(raw.year[i]);
        var ret    = parseFloat(raw.annual_return[i]) || 0;
        if (sector && year) {
          if (!data[sector]) data[sector] = {};
          data[sector][year] = Math.round(ret * 10) / 10;
        }
      }
      return data;
    }

    // Row format (array of objects)
    if (Array.isArray(raw)) {
      raw.forEach(function(row) {
        var sector = row.sector_name || row.sectorName || row.sector;
        var year   = String(row.year);
        var ret    = parseFloat(row.annual_return || row.annualReturn || 0);
        if (sector && year) {
          if (!data[sector]) data[sector] = {};
          data[sector][year] = Math.round(ret * 10) / 10;
        }
      });
      return data;
    }

  } catch (e) {
    console.log("[Heatmap] Error loading data:", e);
  }

  return data;
}

function renderHeatmap() {
  var data = loadData();
  var sectors = Object.keys(data).sort();
  var table = document.getElementById("heatmap-table");
  var legend = document.getElementById("heatmap-legend");

  if (!table || !legend) return;

  // Derive years dynamically from the data
  var YEARS = [];
  sectors.forEach(function(sector) {
    Object.keys(data[sector]).forEach(function(y) {
      if (YEARS.indexOf(y) === -1) YEARS.push(y);
    });
  });
  YEARS.sort();

  if (sectors.length === 0 || YEARS.length === 0) {
    table.innerHTML = '<tr><td style="color:#94a3b8;padding:20px;">No data available. Wire a query to the queryRows state key.</td></tr>';
    return;
  }

  // Build header
  var html = "<thead><tr>";
  html += '<th class="sector-header">SECTOR</th>';
  YEARS.forEach(function(y) {
    html += "<th>" + y + "</th>";
  });
  html += "<th>AVG</th>";
  html += "</tr></thead>";

  // Build body
  html += "<tbody>";
  sectors.forEach(function(sector) {
    var yearData = data[sector] || {};
    var vals = [];
    YEARS.forEach(function(y) {
      if (yearData[y] !== null && yearData[y] !== undefined) {
        vals.push(yearData[y]);
      }
    });
    var avg = vals.length > 0
      ? vals.reduce(function(a, b) { return a + b; }, 0) / vals.length
      : null;

    html += "<tr>";

    var dotColor = SECTOR_COLORS[sector] || "#475569";
    html += '<td class="sector-name">';
    html += '<span class="sector-dot" style="background:' + dotColor + '"></span>';
    html += sector;
    html += "</td>";

    YEARS.forEach(function(y) {
      var v = (yearData[y] !== undefined && yearData[y] !== null) ? yearData[y] : null;
      var bg = heatColor(v);
      var tc = heatText(v);
      html += '<td class="value-cell">';
      html += '<div class="heat-cell" style="background:' + bg + ";color:" + tc + '"';
      html += ' data-sector="' + sector + '" data-year="' + y + '" data-value="' + v + '"';
      html += ' title="' + sector + " " + y + ": " + (v !== null ? v + "%" : "N/A") + '">';
      html += formatVal(v);
      html += "</div></td>";
    });

    var avgBg = avg !== null ? heatColor(avg) : "#1e293b";
    var avgTc = avg !== null ? heatText(avg) : "#475569";
    html += '<td class="avg-cell">';
    html += '<div class="heat-cell-avg" style="background:' + avgBg + ";color:" + avgTc + '">';
    html += formatVal(avg);
    html += "</div></td>";

    html += "</tr>";
  });
  html += "</tbody>";

  table.innerHTML = html;

  // Legend
  var legendHtml = '<span class="legend-label">Scale:</span>';
  LEGEND_ITEMS.forEach(function(item) {
    legendHtml += '<span class="legend-item" style="background:' + item.bg + ";color:" + item.text + '">';
    legendHtml += item.label;
    legendHtml += "</span>";
  });
  legend.innerHTML = legendHtml;

  setupTooltip();
  setupClickHandlers();
}

function setupTooltip() {
  var tip = document.getElementById("heatmap-tooltip");
  if (!tip) {
    tip = document.createElement("div");
    tip.id = "heatmap-tooltip";
    tip.innerHTML = '<div class="tip-sector"></div><div class="tip-value"></div>';
    document.body.appendChild(tip);
  }

  var cells = document.querySelectorAll(".heat-cell");
  cells.forEach(function(cell) {
    cell.addEventListener("mousemove", function(e) {
      var sector = cell.getAttribute("data-sector");
      var year = cell.getAttribute("data-year");
      var value = cell.getAttribute("data-value");

      tip.querySelector(".tip-sector").textContent = sector;

      var valNum = parseFloat(value);
      var valStr = (value && value !== "null") ? formatVal(valNum) + "%" : "N/A";
      var valColor = (value && value !== "null" && valNum >= 0) ? "#4ade80" : "#f87171";

      tip.querySelector(".tip-value").innerHTML =
        year + ': <span style="color:' + valColor + '">' + valStr + "</span>";

      tip.style.display = "block";
      tip.style.left = (e.clientX + 12) + "px";
      tip.style.top = (e.clientY - 28) + "px";
    });

    cell.addEventListener("mouseleave", function() {
      tip.style.display = "none";
    });
  });
}

function setupClickHandlers() {
  var cells = document.querySelectorAll(".heat-cell");
  cells.forEach(function(cell) {
    cell.addEventListener("click", function() {
      var sector = cell.getAttribute("data-sector");
      var year = cell.getAttribute("data-year");
      var value = cell.getAttribute("data-value");
      try {
        SlateFunctions.triggerEvent("cellClick", {
          sector: sector,
          year: year,
          value: parseFloat(value)
        });
      } catch (e) {
        console.log("[Heatmap] Cell clicked:", sector, year, value);
      }
    });
  });
}

renderHeatmap();

try {
  SlateFunctions.onAction("refresh", function() {
    renderHeatmap();
  });
} catch (e) {}
