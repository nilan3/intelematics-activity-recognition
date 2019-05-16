# Intelematics Activity Recognition
Predict user's activity based on accelerometer/gyroscope/magnetometer readings on wrist/chest/ankle

#### Sample Data
Sample data from [here](https://drive.google.com/file/d/1YNG0PPv0lnKKHzDBd3uWq248k7Aj-I8q) to be stored in `src/main/resources/testdata/activity-monitoring/imu_activity_recognition.csv`

## Pipeline
![alt text](https://raw.githubusercontent.com/nilan3/intelematics-activity-recognition/master/pipeline_architecture.png)
### Preprocessing Job - `ActivityRecognitionApplication`
```bash
Main class: com.intelematics.medm.application.activityrecognition.ActivityRecognitionApplication
Program arguments: configurations/telematics_activity_recognition.yml
```
The data currently has many missing parts:
- N rows contains 'NA' for all IMU and HR measurements which can be removed
- 14326 rows contains 'NA' for at least 1 IMU measurement
- HR measurements are sampled at low frequency (11x less data)

Two ways I can think of to tackle this to ensure data which is fed into a model contains values for all fields:
- Split data to blocks (each block represents a unique user-activity combination - this is to isolate data for each activity and prevents averaging/filling across them)
  - Forward feed - for each user, fill in the 'NA' based on the last observed result for a particular measurement.
  - Aggregate and average

Due to performance with Spark, I have chosen to aggregate the data into buckets (size of hr sampling frequency wrt time) and take an average of all measurements. Forward fill requires bring all data partitions together and looping over the list, which is not scalable. Aggregating does however reduce the amount of the training data considerably.

We can simplify the data/model to reduce storage and processing. Each IMU measurements consists of `x`,`y`,`z` components. We can combine all 3 parts to form a vector and calculate the magnitude hence reducing the number of input features for a model.
```scala
math.sqrt((x*x) + (y*y) + (z*z))
```

### Training Job - `MlpClassifierApplication`
```bash
Main class: com.intelematics.medm.application.mlpclassifier.MlpClassifierApplication
Program arguments: configurations/mlp_classification_activity_recognition.yml
```
Data is now ready with all records with filled fields. I have chosen to use a `Multilayer Perceptron Classifier` based on a normal forwardforward neural network. This is included in Spark ML library so can be scaled and easily implemented due to its high level API (`https://spark.apache.org/docs/latest/ml-classification-regression.html#multilayer-perceptron-classifier`).

Input features:
   - heart_rate
   - wrist_accelerometer
   - wrist_gyroscope
   - wrist_magnetometer
   - chest_accelerometer
   - chest_gyroscope
   - chest_magnetometer
   - ankle_accelerometer
   - ankle_gyroscope
   - ankle_magnetometer

Output Label: activity_id

Hidden Layers: 1

![alt text](https://raw.githubusercontent.com/nilan3/intelematics-activity-recognition/master/neural_network.png)

Input features are **standardised** to have a mean of 0 and stdev of 1. This is because measurements are of different units so standardisation can prevent large weight values and high sensitivity to input data.

Once training has completed, scaler and model is saved to HDFS for later use in a streaming pipeline.

### Streaming Job - `ActivityRecognitionStreamingPrediction`
```bash
Main class: com.intelematics.medm.application.activityrecognition.ActivityRecognitionStreamingPrediction
Program arguments: configurations/activity_recognition_streaming_prediction.yml
```
Kafka connectors added but tested with File connector only.
It was later discovered that `MultilayerPerceptronClassifier` does not currently support structured streaming so model cannot be converted to a stream transformer. This job currently picks up new data as it lands in the directory. Similar logic would apply to pre process the data before it can be fed into the model for prediction.

## Future Improvements
- Use tensorflow/keras to build classification Neural Network. May be simpler to even try a decision tree.
  - Use databricks `sparkdl` to integrate tensorflow/keras model into a spark pipeline.
- Implement anomaly detection in the pre processing phase to remove any abnormal data which can affect the neural network.
  - Merge all measurements to form a signature for a user's activity. Use similarity algorithm (e.g. cosine similarity) to identify groups of similar behaviour but most importantly any abnormal ones which can be filtered out before training.
  - or use isolation forest to pick out anomalies
