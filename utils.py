import tensorflow as tf
import pandas as pd
from tensorflow import keras
from keras import layers, metrics

def dataframe_to_tensor_dataset (dataframe, points_first_day_id, num_points_features, num_points_labels, batch_size, audio_features_id = None, num_audio_features = None, followers_id = None):

    """
    Method for transforming a Pandas dataframe to a Tensorflow dataset with the relevant features
    
    :param dataframe: Input dataframe containing all data points and possible features used for training
    :param points_first_day_id: Index of the column in dataframe containing 'Points' on first day
    :param num_points_features: Number for how many days of 'Points' to select as features
    :param num_points_labels: Number for how many days of 'Points' to select as labels
    :param batch_size: Batch size for the Tensorflow dataset
    :param audio_features_id: Index of the first audio feature column in the dataframe, default None
    :param num_audio_features: Number of how many audio features to select as features, default None
    :param followers_id: Index of the total follower column in the dataframe, default None
    :return: The transformed Tensorflow dataset with the relevant features
    """
    features = dataframe.iloc[: ,points_first_day_id: points_first_day_id + num_points_features]

    # Extract the audio features from the dataframe
    if (audio_features_id and num_audio_features) is not None:
        audio_features = dataframe.iloc[: ,audio_features_id:audio_features_id + num_audio_features]
        features = pd.concat([audio_features, features], axis = 1)


    # Extract the follower count from the dataframe
    if followers_id is not None:
        avg_followers = dataframe.iloc[: ,followers_id]
        features = pd.concat([avg_followers ,features], axis = 1)

    # Convert the features to a tensor
    features = features.to_numpy()
    features = features.reshape(features.shape[0], features.shape[1], -1)
    features = tf.convert_to_tensor(features, dtype=tf.float32)

    # Extract the labels (points for the last k days) and convert to a tensor
    label_id = points_first_day_id+ num_points_features
    labels = dataframe.iloc[: ,label_id:label_id + num_points_labels]
    labels = tf.convert_to_tensor(labels, dtype=tf.float32)

    # Join the tensors in a tensor Dataset, create respective batches
    dataset = tf.data.Dataset.from_tensor_slices((features, labels))
    dataset = dataset.batch(batch_size)

    return dataset


def train_model(hidden_units, epochs, train_dataset, valid_dataset = None):

    """
    Method for training a neural network with a single LSTM unit and one fully connected layer
    :param hidden_units: Number of hidden units in the fully connected layer
    :param epochs: Number of epochs to perform the training for
    :param train_dataset: Tensorflow training dataset
    :param valid_dataset: Tensorflow validation dataset, default None
    :return: The training history and the trained model
    """
    days_to_predict = 7
    output_features = 1

    # Construct the model architecture - 1 LSTM unit + 1 fully connected layer
    lstm_model = keras.Sequential([

        layers.LSTM(hidden_units, return_sequences=False),

        layers.Dense(days_to_predict * output_features,
                     kernel_initializer=tf.initializers.zeros()
                     ),

        layers.Reshape([days_to_predict, output_features])
    ])

    # Use MSE to measure loss and Adam optimizer
    lstm_model.compile(loss=keras.losses.MeanSquaredError(),
                       optimizer=keras.optimizers.Adam(),
                       metrics=[keras.metrics.MeanAbsoluteError()])

    # Perform model training
    training_history = lstm_model.fit(train_dataset, epochs=epochs,
                                      validation_data=valid_dataset, verbose=0
                                      )

    return training_history.history, lstm_model


# Define the baseline model
class BaselineRepeatLast(tf.keras.Model):
  def call(self, inputs):
    last_time_step = inputs[:, -1:, :]
    return tf.tile(last_time_step, [1, 7, 1])


class LinearBaseline(tf.keras.Model):
    def __init__(self):
        super(LinearBaseline, self).__init__()

    def points_8_to_14(self, item):

        # Obtain the points on first and seventh day
        first_day_points = item[0]
        seventh_day_points = item[6]

        # Calculate the slope and the intercept
        slope = (seventh_day_points - first_day_points) / 6
        intercept = first_day_points - slope

        # Predict the points for the next 7 days using the linear function
        next_days = tf.range(8, 15, dtype=tf.float32)
        next_day_points = next_days * slope + intercept

        next_day_points = tf.clip_by_value(next_day_points, 0.0, 200.0)

        return next_day_points

    def linear_function(self, inputs):
        outputs = tf.map_fn(self.points_8_to_14, inputs)
        outputs = tf.expand_dims(outputs, axis=-1)
        return outputs

    def call(self, inputs):
        return self.linear_function(inputs)


def R2_Score(predicted_values, target_values):
    r2_metric = metrics.R2Score()
    r2_metric.update_state(target_values, predicted_values)
    r2_score = r2_metric.result().numpy()
    return r2_score