// Copyright 2023 Zinc Labs Inc.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at

//      http:www.apache.org/licenses/LICENSE-2.0

//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

import moment from "moment";
import { formatDate, formatUnitValue, getUnitValue } from "./convertDataIntoUnitValue";
import { utcToZonedTime } from "date-fns-tz";


// function autoFontSize() {
//   let width = document?.getElementById('chart1')?.offsetWidth || 400;
//   let newFontSize = Math.round(width / 30);
//   console.log(`Current width : ${width}, Updating Fontsize to ${newFontSize}`);
//   return newFontSize;
// };

// it is used to create grid array for gauge chart
function calculateGridPositions(width: any, height: any, numGrids: any) {
  const gridArray: any = [];

  // if no grid then return empty array
  if (numGrids <= 0) {
    return gridArray;
  }

  // if only one grid then return single grid array, width, height, gridNoOfRow, gridNoOfCol
  if(numGrids == 1){
    return {
      gridArray: [{
      left: "0%",
      top: "0%",
      width: "100%",
      height: "100%",
    }],
    gridWidth : width,
    gridHeight : height,
    gridNoOfRow: 1,
    gridNoOfCol: 1
  }
  }

  // total area of chart element
  const totalArea = width * height;
  // per gauge area
  const perChartArea = Math.sqrt(totalArea / numGrids);  
  // number of row and column for gauge rendering
  let numRows = Math.ceil(height / perChartArea);
  let numCols = Math.ceil(width / perChartArea);
  
  // width and height for single gauge
  const cellWidth = 100 / numCols;
  const cellHeight = 100 / numRows;

  // will create 2D grid array
  for (let row = 0; row < numRows; row++) {
    for (let col = 0; col < numCols; col++) {
      const grid = {
        left: `${col * cellWidth}%`,
        top: `${row * cellHeight}%`,
        width: `${cellWidth}%`,
        height: `${cellHeight}%`,
      };
      gridArray.push(grid);
    }
  }

  // return grid array, width, height, gridNoOfRow, gridNoOfCol
  return {
    gridArray: gridArray,
    gridWidth : (cellWidth * width) / 100,
    gridHeight : (cellHeight * height) / 100,
    gridNoOfRow: numRows,
    gridNoOfCol: numCols
  }
}

/**
 * Converts PromQL data into a format suitable for rendering a chart.
 *
 * @param {any} panelSchema - the panel schema object
 * @param {any} searchQueryData - the search query data
 * @param {any} store - the store object
 * @return {Object} - the option object for rendering the chart
 */
export const convertPromQLData = (
  panelSchema: any,
  searchQueryData: any,
  store: any,
  chartPanelRef: any
) => {
  
  // if no data than return it
  if (
    !Array.isArray(searchQueryData) ||
    searchQueryData.length === 0 ||
    !searchQueryData[0] ||
    !panelSchema
  ) {
    return { options: null };
  }

  // It is used to keep track of the current series name in tooltip to bold the series name
  let currentSeriesName = "";

  // set the current series name (will be set at chartrenderer on mouseover)
  const setCurrentSeriesValue = (newValue: any) => {
    currentSeriesName = newValue ?? "";
  };

  const legendPosition = getLegendPosition(
    panelSchema?.config?.legends_position
  );

  const legendConfig: any = {
    show: panelSchema.config?.show_legends,
    type: "scroll",
    orient: legendPosition,
    padding: [10, 20, 10, 10],
    tooltip: {
      show: true,
      padding: 10,
      textStyle: {
        fontSize: 12,
      },
    },
    textStyle: {
      width: 150,
      overflow: "truncate",
      rich: {
        a: {
            fontWeight: 'bold'
        },
        b: {
            fontStyle: 'normal'
        }
      }
    },
    formatter: (name: any) => {
      return name == currentSeriesName ? '{a|' + name + '}': '{b|' + name + '}'
    }
  };

  // Additional logic to adjust the legend position
  if (legendPosition === "vertical") {
    legendConfig.left = null; // Remove left positioning
    legendConfig.right = 0; // Apply right positioning
    legendConfig.top = "center"; // Apply bottom positioning
  } else {
    legendConfig.left = "0"; // Apply left positioning
    legendConfig.top = "bottom"; // Apply bottom positioning
  }

  const options: any = {
    backgroundColor: "transparent",
    legend: legendConfig,
    grid: {
      containLabel: true,
      left: "30",
      right:
        legendConfig.orient === "vertical" && panelSchema.config?.show_legends
          ? 220
          : "40",
      top: "15",
      bottom: "30",
    },
    tooltip: {
      show: true,
      trigger: "axis",
      textStyle: {
        color: store.state.theme === "dark" ? "#fff" : "#000",
        fontSize: 12,
      },
      enterable: true,
      backgroundColor: store.state.theme === "dark" ? "rgba(0,0,0,1)" : "rgba(255,255,255,1)",
      extraCssText: "max-height: 200px; overflow: auto; max-width: 500px",
      formatter: function (name: any) {
        if (name.length == 0) return "";

        const date = new Date(name[0].data[0]);

        // get the current series index from name
        const currentSeriesIndex = name.findIndex(
          (it: any) => it.seriesName == currentSeriesName
        )

        // swap current hovered series index to top in tooltip
        const temp = name[0];
        name[0] = name[currentSeriesIndex != -1 ? currentSeriesIndex : 0];
        name[currentSeriesIndex != -1 ? currentSeriesIndex : 0] = temp;

        let hoverText = name.map((it: any) => {

          // check if the series is the current series being hovered
          // if have than bold it
          if(it?.seriesName == currentSeriesName)
            return `<strong>${it.marker} ${it.seriesName} : ${formatUnitValue(
              getUnitValue(
                it.data[1],
                panelSchema.config?.unit,
                panelSchema.config?.unit_custom
              )
            )} </strong>`;
          // else normal text
          else
            return `${it.marker} ${it.seriesName} : ${formatUnitValue(
              getUnitValue(
                it.data[1],
                panelSchema.config?.unit,
                panelSchema.config?.unit_custom
              )
            )}`;
        });

        return `${formatDate(date)} <br/> ${hoverText.join("<br/>")}`;
      },
      axisPointer: {
        show: true,
        type: "cross",
        label: {
          fontSize: 12,
          show: true,
          formatter: function (name: any) {
            if (name.axisDimension == "y")
              return formatUnitValue(
                getUnitValue(
                  name.value,
                  panelSchema.config?.unit,
                  panelSchema.config?.unit_custom
                )
              );
            const date = new Date(name.value);
            return `${formatDate(date)}`;
          },
        },
      },
    },
    xAxis: {
      type: "time",
    },
    yAxis: {
      type: "value",
      axisLabel: {
        formatter: function (name: any) {
          return formatUnitValue(
            getUnitValue(
              name,
              panelSchema.config?.unit,
              panelSchema.config?.unit_custom
            )
          );
        },
      },
      axisLine: {
        show: true,
      },
    },
    toolbox: {
      orient: "vertical",
      show: !["pie", "donut", "metric", "gauge"].includes(panelSchema.type),
      feature: {
        dataZoom: {
          filterMode: 'none',
          yAxisIndex: "none",
        },
      },
    },
    series: [],
  };

  // to pass grid index in gauge chart
  let gaugeIndex = 0;

  // for gauge chart we need total no. of gauge to calculate grid positions
  let totalLength = 0;
  // for gauge chart, it contains grid array, single chart height and width, no. of charts per row and no. of columns
  let gridDataForGauge: any = {};
  
  if(panelSchema.type === "gauge"){
    // calculate total length of all metrics
    searchQueryData.forEach((metric: any) => {
      if (metric.result && Array.isArray(metric.result)) {
        totalLength += metric.result.length;
      }
    });

    // create grid array based on chart panel width, height and total no. of gauge
    gridDataForGauge = calculateGridPositions(
      chartPanelRef.value.offsetWidth,
      chartPanelRef.value.offsetHeight,
      totalLength
    );

    //assign grid array to gauge chart options
    options.grid = gridDataForGauge.gridArray;
  }
  options.series = searchQueryData.map((it: any, index: number) => {

    switch (panelSchema.type) {
      case "bar":
      case "line":
      case "area":
      case "scatter":
      case "area-stacked": {
        switch (it?.resultType) {
          case "matrix": {
            const seriesObj = it?.result?.map((metric: any) => {
              const values = metric.values.sort(
                (a: any, b: any) => a[0] - b[0]
              );
              return {
                name: getPromqlLegendName(
                  metric.metric,
                  panelSchema.queries[index].config.promql_legend
                ),
                // if utc then simply return the values by removing z from string
                // else convert time from utc to zoned
                // used slice to remove Z from isostring to pass as a utc
                data: values.map((value: any) => [store.state.timezone != "UTC" ? utcToZonedTime(value[0] * 1000, store.state.timezone) : new Date(value[0] * 1000).toISOString().slice(0, -1), value[1]]),
                ...getPropsByChartTypeForSeries(panelSchema.type),
              };
            });

            return seriesObj;
          }
          case "vector": {
            const traces = it?.result?.map((metric: any) => {
              const values = [metric.value];
              return {
                name: JSON.stringify(metric.metric),
                x: values.map((value: any) =>
                  moment(value[0] * 1000).toISOString(true)
                ),
                y: values.map((value: any) => value[1]),
              };
            });
            return traces;
          }
        }
      }

      case "gauge" :{
        const series = it?.result?.map((metric: any) => {          
            const values = metric.values.sort(
              (a: any, b: any) => a[0] - b[0]
            );
            const unitValue = getUnitValue(
              values[values.length - 1][1],
              panelSchema.config?.unit,
              panelSchema.config?.unit_custom
            );
            gaugeIndex++;           
            return {
              ...getPropsByChartTypeForSeries(panelSchema.type),
              min: panelSchema?.config?.min || 0,
              max: panelSchema?.config?.max || 100,

              //which grid will be used
              gridIndex: gaugeIndex - 1,
              // radius, progress and axisline width will be calculated based on grid height
              radius: `${options.grid[gaugeIndex - 1].height}`,
              progress: {
                show: true,
                width: parseFloat(options.grid[gaugeIndex - 1].height)/2,
              },
              axisLine: {
                lineStyle: {
                  width: parseFloat(options.grid[gaugeIndex - 1].height)/2,
                }
              },
              title:{
                fontSize: 10,
                offsetCenter: [0, "70%"],
                // width: upto chart width
                width: `${gridDataForGauge.gridWidth}`,
                overflow: "truncate"
              },

              // center of gauge
              // x: left + width / 2,
              // y: top + height / 2,
              center:[`${parseFloat(options.grid[gaugeIndex - 1].left) + parseFloat(options.grid[gaugeIndex - 1].width) / 2}%`, `${parseFloat(options.grid[gaugeIndex - 1].top) + parseFloat(options.grid[gaugeIndex - 1].height) / 2}%`],
              data:[{
                name: JSON.stringify(metric.metric),
                value:parseFloat(unitValue.value).toFixed(2),
                detail: {
                  formatter: function (value:any) {
                    return value + unitValue.unit;
                  },
                }
              }],
              detail: {
                valueAnimation: true,
                offsetCenter: [0, 0],
                fontSize:12,
              },
            };
          });
          options.dataset = { source: [[]] };
          options.tooltip = {
            show: true,
            trigger: "item",
          };
          options.angleAxis = {
            show: false,
          };
          options.radiusAxis = {
            show: false,
          };
          options.polar = {};
          options.xAxis = [];
          options.yAxis = [];
          return series;
      }
      
      case "metric": {
        switch (it?.resultType) {
          case "matrix": {
            const series = it?.result?.map((metric: any) => {
              const values = metric.values.sort(
                (a: any, b: any) => a[0] - b[0]
              );
              const unitValue = getUnitValue(
                values[values.length - 1][1],
                panelSchema.config?.unit,
                panelSchema.config?.unit_custom
              );
              return {
                ...getPropsByChartTypeForSeries(panelSchema.type),
                renderItem: function (params: any) {
                  return {
                    type: "text",
                    style: {
                      text:
                        parseFloat(unitValue.value).toFixed(2) + unitValue.unit,
                      fontSize: Math.min(params.coordSys.cx / 2, 90), //coordSys is relative. so that we can use it to calculate the dynamic size
                      fontWeight: 500,
                      align: "center",
                      verticalAlign: "middle",
                      x: params.coordSys.cx,
                      y: params.coordSys.cy,
                      fill: store.state.theme == "dark" ? "#fff" : "#000",
                    },
                  };
                },
              };
            });
            options.dataset = { source: [[]] };
            options.tooltip = {
              show: false,
            };
            options.angleAxis = {
              show: false,
            };
            options.radiusAxis = {
              show: false,
            };
            options.polar = {};
            options.xAxis = [];
            options.yAxis = [];
            return series;
          }
          case "vector": {
            const traces = it?.result?.map((metric: any) => {
              const values = [metric.value];
              return {
                name: JSON.stringify(metric.metric),
                value: metric?.value?.length > 1 ? metric.value[1] : "",
                ...getPropsByChartTypeForSeries(panelSchema.type),
              };
            });
            return traces;
          }
        }
        break;
      }
    default: {
      return [];
    }
  }
});

  options.series = options.series.flat();

  // extras will be used to return other data to chart renderer
  // e.g. setCurrentSeriesValue to set the current series index which is hovered
  return { options, extras: { setCurrentSeriesValue }};
};

/**
 * Retrieves the legend name for a given metric and label.
 *
 * @param {any} metric - The metric object containing the values for the legend name placeholders.
 * @param {string} label - The label template for the legend name. If null or empty, the metric object will be converted to a JSON string and returned.
 * @return {string} The legend name with the placeholders replaced by the corresponding values from the metric object.
 */
const getPromqlLegendName = (metric: any, label: string) => {
  if (label) {
    let template = label || "";
    const placeholders = template.match(/\{([^}]+)\}/g);

    // Step 2: Iterate through each placeholder
    placeholders?.forEach(function (placeholder: any) {
      // Step 3: Extract the key from the placeholder
      const key = placeholder.replace("{", "").replace("}", "");

      // Step 4: Retrieve the corresponding value from the JSON object
      const value = metric[key];

      // Step 5: Replace the placeholder with the value in the template
      if (value) {
        template = template.replace(placeholder, value);
      }
    });
    return template;
  } else {
    return JSON.stringify(metric);
  }
};

/**
 * Determines the position of the legend based on the provided legendPosition.
 *
 * @param {string} legendPosition - The desired position of the legend. Possible values are "bottom" or "right".
 * @return {string} The position of the legend. Possible values are "horizontal" or "vertical".
 */
const getLegendPosition = (legendPosition: string) => {
  switch (legendPosition) {
    case "bottom":
      return "horizontal";
    case "right":
      return "vertical";
    default:
      return "horizontal";
  }
};


/**
 * Returns the props object based on the given chart type.
 *
 * @param {string} type - The chart type.
 * @return {object} The props object for the given chart type.
 */
const getPropsByChartTypeForSeries = (type: string) => {
  switch (type) {
    case "bar":
      return {
        type: "bar",
        emphasis: { focus: "series" },
      };
    case "line":
      return {
        type: "line",
        emphasis: { focus: "series" },
        smooth: true,
        showSymbol: false,
      };
    case "scatter":
      return {
        type: "scatter",
        emphasis: { focus: "series" },
        symbolSize: 5,
      };
    case "pie":
      return {
        type: "pie",
        emphasis: { focus: "series" },
      };
    case "donut":
      return {
        type: "pie",
        emphasis: { focus: "series" },
      };
    case "h-bar":
      return {
        type: "bar",
        orientation: "h",
        emphasis: { focus: "series" },
      };
    case "area":
      return {
        type: "line",
        emphasis: { focus: "series" },
        smooth: true,
        areaStyle: {},
        showSymbol: false,
      };
    case "stacked":
      return {
        type: "bar",
        emphasis: { focus: "series" },
      };
    case "area-stacked":
      return {
        type: "line",
        smooth: true,
        stack: "Total",
        areaStyle: {},
        showSymbol: false,
        emphasis: {
          focus: "series",
        },
      };
    case "gauge":
      return {
          type: 'gauge',
          startAngle: 205,
          endAngle: -25,
          pointer: {
            show: false
          },
          axisTick: {
            show:false
          },
          splitLine: {
            show:false
          },
          axisLabel: {
            show:false
          }
      };
    case "metric":
      return {
        type: "custom",
        coordinateSystem: "polar",
      };
    case "h-stacked":
      return {
        type: "bar",
        emphasis: { focus: "series" },
        orientation: "h",
      };
    default:
      return {
        type: "bar",
      };
  }
};
