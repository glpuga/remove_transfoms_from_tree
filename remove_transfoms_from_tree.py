#!/bin/python3

import rosbag2_py
import rclpy
from rosidl_runtime_py.utilities import get_message
from rclpy.serialization import deserialize_message, serialize_message

import argparse
import sys


class TransformFilter:

    # constructor that takes the input file, the output file, the storge id, the list of topics to filter on, and the list of transform pairs to filter out
    def __init__(self, storage_id, topics, transforms, serialization_format):
        self.storage_id = storage_id
        self.topics = topics
        self.transforms = transforms
        self.serialization_format = serialization_format

    def get_rosbag_options(self, path, storage_id, serialization_format):
        storage_options = rosbag2_py.StorageOptions(uri=path, storage_id=storage_id)
        converter_options = rosbag2_py.ConverterOptions(
            input_serialization_format=serialization_format,
            output_serialization_format=serialization_format,
        )
        return storage_options, converter_options

    def create_topic(self, writer, id, topic_name, topic_type, serialization_format):
        topic_name = topic_name
        topic = rosbag2_py.TopicMetadata(
            id=id,
            name=topic_name,
            type=topic_type,
            serialization_format=serialization_format,
        )
        writer.create_topic(topic)

    def filter(self, inputbag, outputbag):
        storage_options, converter_options = self.get_rosbag_options(
            inputbag, self.storage_id, self.serialization_format
        )
        storage_options2, converter_options2 = self.get_rosbag_options(
            outputbag, self.storage_id, self.serialization_format
        )

        reader = rosbag2_py.SequentialReader()
        reader.open(storage_options, converter_options)

        writer = rosbag2_py.SequentialWriter()
        writer.open(storage_options2, converter_options2)

        topic_types = reader.get_all_topics_and_types()
        type_map = {
            topic_types[i].name: topic_types[i].type for i in range(len(topic_types))
        }

        for i in range(len(topic_types)):
            self.create_topic(
                writer,
                topic_types[i].id,
                topic_types[i].name,
                topic_types[i].type,
                self.serialization_format,
            )

        while reader.has_next():
            (topic, data, t) = reader.read_next()
            msg_type = get_message(type_map[topic])
            msg = deserialize_message(data, msg_type)

            if topic in self.topics:
                # this assumes that the chosen topics have transform types only, and that
                # only a single transform populated in the message array
                parent = msg.transforms[0].header.frame_id
                child = msg.transforms[0].child_frame_id
                if not (
                    (parent, child) in self.transforms
                    or (child, parent) in self.transforms
                ):
                    writer.write(topic, serialize_message(msg), t)
            else:
                if topic == "/velocity_control/odom":
                    # make up a transform from this message
                    mumsg = get_message("tf2_msgs/msg/TFMessage")()
                    mumsg.transforms.append(
                        get_message("geometry_msgs/msg/TransformStamped")()
                    )
                    mumsg.transforms[0].header.stamp.sec = msg.header.stamp.sec
                    mumsg.transforms[0].header.stamp.nanosec = msg.header.stamp.nanosec
                    mumsg.transforms[0].header.frame_id = msg.header.frame_id
                    mumsg.transforms[0].child_frame_id = msg.child_frame_id
                    mumsg.transforms[0].transform.translation.x = msg.pose.pose.position.x
                    mumsg.transforms[0].transform.translation.y = msg.pose.pose.position.y
                    mumsg.transforms[0].transform.translation.z = msg.pose.pose.position.z
                    mumsg.transforms[0].transform.rotation.x = msg.pose.pose.orientation.x
                    mumsg.transforms[0].transform.rotation.y = msg.pose.pose.orientation.y
                    mumsg.transforms[0].transform.rotation.z = msg.pose.pose.orientation.z
                    mumsg.transforms[0].transform.rotation.w = msg.pose.pose.orientation.w

                    writer.write(topic, serialize_message(msg), t)
                    writer.write("/tf", serialize_message(mumsg), t)

                else:
                    writer.write(topic, serialize_message(msg), t)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(sys.argv[0])

    # add argument for the list of bag files to be filtered
    parser.add_argument(
        "--inputbag",
        dest="inputbag",
        required=True,
        nargs="+",
        help="The input bag files",
    )

    # add argument for the output dir to store the filtered bag files
    parser.add_argument(
        "--outputdir",
        dest="outputdir",
        default="",
        help="The output directory to store the filtered bag files",
    )

    # add a list of transform pairs to be filtered out
    parser.add_argument(
        "--remove",
        dest="filter",
        nargs="+",
        default=[],
        help='A list of transform pairs to filter out. Write pairs as "frame_id:child_frame_id"',
    )

    # add a list of topics to filter on, and make /tf the default
    parser.add_argument(
        "--topics",
        dest="topics",
        nargs="+",
        default=[
            "/tf",
        ],
        help="A list of topics to filter on",
    )

    # add an argument to set the storage format, use "" as default
    parser.add_argument(
        "--storage_format",
        dest="storage_format",
        default="",
        help="The storage format to use for the output bag file",
    )
    args = parser.parse_args()

    # split the list of transform pairs into a list of tuples
    transform_pairs = [(x.split(":")[0], x.split(":")[1]) for x in args.filter]

    # crate the TransformFilter object
    filter = TransformFilter(
        args.storage_format,
        args.topics,
        transform_pairs,
        "cdr",
    )

    for inputbag in args.inputbag:
        if args.outputdir:
            outputbag = args.outputdir + "/" + inputbag
        else:
            outputbag = inputbag + "_filtered"

        print(f"Filtering {inputbag} to {outputbag}...")

        filter.filter(inputbag, outputbag)

    exit(1)
