#!/usr/bin/env python3
import os
import json
import tweepy
import requests
from datetime import datetime, timedelta

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform


REQUIRED_CONFIG_KEYS = ["start_date", "refresh_token", "client_id", "client_secret"]
HOST = "https://api.twitter.com/2"
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def get_key_properties(stream_name):
    key_properties = {
        "user_tweets": ["id"]
    }
    return key_properties[stream_name]


def create_metadata_for_report(schema, tap_stream_id):
    key_properties = get_key_properties(tap_stream_id)

    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "FULL_TABLE"}}]

    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    for key in schema.properties:
        if "object" in schema.properties.get(key).type:
            for prop in schema.properties.get(key).properties:
                inclusion = "automatic" if prop in key_properties else "available"
                mdata.extend([{
                    "breadcrumb": ["properties", key, "properties", prop],
                    "metadata": {"inclusion": inclusion}
                }])
        else:
            inclusion = "automatic" if key in key_properties else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(schema, stream_id)
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


def print_metrics(config):
    creds = {
        "raw_credentials": {"refresh_token": config["refresh_token"]}
    }
    metric = {"type": "secret", "value": creds, "tags": "tap-secret"}
    LOGGER.info('METRIC: %s', json.dumps(metric))


def _refresh_token(config):
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    data = {
        'client_id': config['client_id'],
        'grant_type': 'refresh_token',
        'refresh_token': config['refresh_token']
    }

    url = HOST + "/oauth2/token"
    response = requests.post(url, headers=headers, data=data, auth=(config["client_id"], config["client_secret"]))

    return response.json()


def refresh_access_token_if_expired(config):
    # if [expires_at not exist] or if [exist and less than current time] then it will update the token
    if config.get('expires_at') is None or config.get('expires_at') < datetime.utcnow():
        res = _refresh_token(config)
        config["access_token"] = res["access_token"]
        config["refresh_token"] = res["refresh_token"]
        config["expires_at"] = datetime.utcnow() + timedelta(seconds=res["expires_in"])
        print_metrics(config)
        return True
    return False


def request_data(config):
    refresh_access_token_if_expired(config)
    client = tweepy.Client(config["access_token"])
    user = requests.get(HOST + "/users/me",
                        headers={'Authorization': f'Bearer {config["access_token"]}'}).json()["data"]

    all_tweets = []
    all_attachment_media = {}
    next_token = None
    first_call = True
    while next_token or first_call:
        # DP: Should there be promoted metrics here as well?
        tweets = client.get_users_tweets(id=user["id"],
                                         expansions=["attachments.media_keys", "author_id"],
                                         media_fields=["public_metrics", "url", "duration_ms", "height", "width", "alt_text"],
                                         tweet_fields=["public_metrics", "non_public_metrics", "organic_metrics",
                                                       "created_at"],
                                         start_time=config["start_date"],
                                         pagination_token=next_token)
        if tweets.errors:
            raise Exception(str(tweets.errors))

        if tweets.data:
            all_tweets += tweets.data
            all_attachment_media.update({m.media_key: m.data for m in tweets.includes.get("media", [])})
            
        next_token = tweets.meta.get("next_token")

        if first_call:
            first_call = False

    all_tweets = [t.data for t in all_tweets]
    for tweet in all_tweets:
        media_keys = tweet.get("attachments", {}).get("media_keys", [])
        tweet["media"] = [all_attachment_media[k] for k in media_keys]
        tweet["author_name"] = user["username"]

    return all_tweets


def sync_streams(config, state, stream):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    records = request_data(config)

    with singer.metrics.record_counter(stream.tap_stream_id) as counter:
        for row in records:
            # Type Conversation and Transformation
            transformed_data = transform(row, schema, metadata=mdata)

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [transformed_data])
            counter.increment()


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        sync_streams(config, state, stream)
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        state = args.state or {}
        sync(args.config, state, catalog)


if __name__ == "__main__":
    main()

