const { InfluxDB } = require('@influxdata/influxdb-client');
const fs = require('fs');

/** Environment variables **/
const url = 'http://localhost:8086'; //change this to http://localhost:8086 if the pc you are using has influxdb set up
const token = "JsPit7YGMRc49iJXz9TGannNogIpwXbV6JfXbmRhV_KcxKbBOmtg8OJY7oU2e78vTkg4BAAYh4q0ElovwepM-g==";
const org = "USA";
//const bucket = "terasort1/autogen";

// Create InfluxDB client
const influxDB = new InfluxDB({ url, token });
const queryApi = influxDB.getQueryApi(org);

let data = []; // array to store the data
let measurements = [
  'attachdetach_controller_forced_detaches', 'bootstrap_signer_rate_limiter_use', 'container_cpu_cfs_periods_total',
  'container_spec_cpu_period', 'container_spec_cpu_quota', 'container_spec_cpu_shares', 'machine_cpu_cores',
  'node_cpu_core_throttles_total', 'node_cpu_seconds_total', 'node_hwmon_chip_names', 'node_hwmon_temp_max_celsius',
  'process_cpu_seconds_total', 'scheduler_binding_duration_seconds_bucket', 'scheduler_binding_latency_microseconds_sum',
  'scheduler_e2e_scheduling_duration_seconds_bucket', 'scheduler_e2e_scheduling_latency_microseconds_sum',
  'scheduler_scheduling_algorithm_duration_seconds_bucket', 'scheduler_scheduling_latency_seconds_sum'
];
const labelMap = {
  'terasort1': 1, 'terasort2': 1, 'terasort3': 1, 'terasort4': 1, 'terasort5': 1,
  'rf1': 2, 'rf2': 2, 'rf3': 2, 'rf4': 2, 'rf5': 2,
  'svd1': 3, 'svd2': 3, 'svd3': 3, 'svd4': 3, 'svd5': 3,
  'wc1': 4, 'wc2': 4, 'wc3': 4, 'wc4': 4, 'wc5': 4,
};

const fluxQuery = (i, j) => `
  import "experimental"
  import "math"

  measurement1_name = "${measurements[j]}"

  bucket1 =  "terasort${i}/autogen"
  node_name1 = "kubeslave1"
  bucket_name1 = "terasort${i}"

  bucket2 =  "rf${i}/autogen"
  node_name2 = "kubeslave1"
  bucket_name2 = "rf${i}"

  bucket3 = "wc${i}/autogen"
  node_name3 = "kubeslave1"
  bucket_name3 = "wc${i}"

  bucket4 =  "svd${i}/autogen"
  node_name4 = "kubeslave1"
  bucket_name4 = "svd${i}"

  full_name1 = bucket1 
  full_name2 = bucket2 
  full_name3 = bucket3 
  full_name4 = bucket4

  dataFrequency = 30s
  dataFreq = 30

  getData= (bucketName, measurementName, nodeName, workBenchNumber) => {
    data = from(bucket: bucketName)
      |> range(start: 2019-12-09T01:52:56Z, stop: 2019-12-13T01:52:56Z)
      |> filter(fn: (r) => r["_measurement"] == measurementName)
      |> filter(fn: (r) => r["__name__"] == measurementName)
      |> set(key: "WorkBench", value: workBenchNumber)
      |> group(columns: ["WorkBench"])
      |> aggregateWindow(every: dataFrequency, fn: mean, createEmpty: false)
      |> map(fn: (r) => ({ _time: r._time, WorkBench: r.WorkBench, _value: r._value }))
    return data
  }

  t1 = getData(bucketName: bucket1, measurementName: measurement1_name, nodeName: node_name1, workBenchNumber: bucket_name1)
  t2 = getData(bucketName: bucket2, measurementName: measurement1_name, nodeName: node_name2, workBenchNumber: bucket_name2)
  t3 = getData(bucketName: bucket3, measurementName: measurement1_name, nodeName: node_name3, workBenchNumber: bucket_name3)
  t4 = getData(bucketName: bucket4, measurementName: measurement1_name, nodeName: node_name4, workBenchNumber: bucket_name4)

  allData1 = union(tables: [t1, t2, t3, t4])
    |> experimental.alignTime(alignTo: 2019-12-09T01:52:56Z)

  count = allData1
    |> count(column: "_value")
    |> map(fn: (r) => ({WorkBench: r.WorkBench, _value: float(v:r._value)}))
    |> set(key: "Type", value: "Count")

  sum = allData1
    |> reduce(fn: (r, accumulator) => ({sum: r._value + accumulator.sum}), identity: {sum: 0.0})
    |> map(fn: (r) => ({WorkBench: r.WorkBench, _value: r.sum}))
    |> set(key: "Type", value: "Sum")

  percentile75 = allData1
    |> quantile(column: "_value", q: 0.75, method: "estimate_tdigest")
    |> map(fn: (r) => ({WorkBench: r.WorkBench, _value: r._value}))
    |> set(key: "Type", value: "75 percentile")

  percentile50 = allData1
    |> quantile(column: "_value", q: 0.50, method: "estimate_tdigest")
    |> map(fn: (r) => ({WorkBench: r.WorkBench, _value: r._value}))
    |> set(key: "Type", value: "50 percentile")

  percentile25 = allData1
    |> quantile(column: "_value", q: 0.25, method: "estimate_tdigest")
    |> map(fn: (r) => ({WorkBench: r.WorkBench, _value: r._value}))
    |> set(key: "Type", value: "25 percentile")

  duration = allData1
    |> reduce(fn: (r, accumulator) => ({_value: float(v:dataFreq) + accumulator._value}), identity: {_value: 0.0})
    |> set(key: "Type", value: "duration(estimate)")

  meanStats = allData1
    |> mean(column: "_value")
    |> map(fn: (r) => ({WorkBench: r.WorkBench, _value: float(v:r._value)}))
    |> set(key: "Type", value: "Mean")

  stddevStats = allData1
    |> stddev(column: "_value")
    |> map(fn: (r) => ({WorkBench: r.WorkBench, _value: float(v:r._value)}))
    |> set(key: "Type", value: "Deviation")

  medianStats = allData1
    |> median(column: "_value")
    |> map(fn: (r) => ({WorkBench: r.WorkBench, _value: float(v:r._value)}))
    |> set(key: "Type", value: "Median")

  minStats = allData1
    |> min(column: "_value")
    |> map(fn: (r) => ({WorkBench: r.WorkBench, _value: float(v:r._value)}))
    |> set(key: "Type", value: "Min")

  maxStats = allData1
    |> max(column: "_value")
    |> map(fn: (r) => ({WorkBench: r.WorkBench, _value: float(v:r._value)}))
    |> set(key: "Type", value: "Max")

  combinedStats = union(tables: [count, sum, percentile75, percentile50, percentile25, duration, meanStats, medianStats, maxStats, minStats, stddevStats])
    |> group()
    |> pivot(rowKey: ["WorkBench"], columnKey: ["Type"], valueColumn: "_value")
    |> yield(name: "fhsadas")
`;

const myQuery = async (i,j) => {
  const query = fluxQuery(i,j);
  for await (const { values, tableMeta } of queryApi.iterateRows(query)) {
    const o = tableMeta.toObject(values);
    data.push({
      WorkBench: o.WorkBench,
      Measurement: measurements[j],
      Count: o.Count,
      Sum: o.Sum,
      Duration: o["duration(estimate)"],
      Deviation: o.Deviation,
      Mean: o.Mean,
      Median: o.Median,
      Max: o.Max,
      Min: o.Min,
      Percentile75: o["75 percentile"],
      Percentile50: o["50 percentile"],
      Percentile25: o["25 percentile"],
      Label: labelMap[o.WorkBench],
    });
  }
};

const executeQuery = async () => {
for(let j = 0;j < measurements.length; j++){

  //change the loop as necessary to get the runs you need
    for (let i = 1; i <= 4; i++) { //iterate over 5 runs

      if(i===3){
        //skip the third run if needed since it has some outliers
      }
      else{
      await myQuery(i,j);
      }
    }
    // Log data after all queries are complete
    //console.log(data);
    
  };
 // Write data to a file after all queries are complete
 const jsonData = JSON.stringify(data, null, 2);
 fs.writeFileSync('TrainTest3.json', jsonData, 'utf8');
 console.log("Data has been written to query_results.json");

}

/** Execute a query and receive line table metadata and rows. */
executeQuery();
