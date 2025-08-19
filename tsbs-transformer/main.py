from quixstreams import Application

import os


def unpack_metrics(row: dict):
    row.update(row.pop('fields'))
    return row


def main():
    """
    Transformations generally read from, and produce to, Kafka topics.

    They are conducted with Applications and their accompanying StreamingDataFrames
    which define what transformations to perform on incoming data.

    Be sure to explicitly produce output to any desired topic(s); it does not happen
    automatically!

    To learn about what operations are possible, the best place to start is:
    https://quix.io/docs/quix-streams/processing.html
    """

    # Setup necessary objects
    app = Application(
        consumer_group="quixlake_transformer",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    input_topic = app.topic(name=os.environ["input"])
    output_topic = app.topic(name=os.environ["output"], key_serializer='str')
    sdf = app.dataframe(topic=input_topic)

    for tag in ['region', 'datacenter', 'hostname']:
        sdf[tag] = sdf['tags'][tag]
    sdf = sdf.set_timestamp(lambda row, *_: row['timestamp'] // 1000000)
    sdf = sdf.drop(['tags', 'measurement', 'timestamp']).apply(unpack_metrics)
    sdf.print(metadata=True)
    sdf.to_topic(
        output_topic,
        key=lambda row: '__'.join(
            [row['region'], row['datacenter'], row['hostname']]
        )
    )

    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()
