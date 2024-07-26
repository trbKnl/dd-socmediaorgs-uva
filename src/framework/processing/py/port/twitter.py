
"""
DDP Twitter module

This module contains functions to handle *.js files contained within a twitter ddp
"""

from pathlib import Path
from typing import Any
import logging
import zipfile
import math
import json
import io
import re

import pandas as pd

import port.unzipddp as unzipddp
import port.helpers as helpers
from port.validate import (
    DDPCategory,
    StatusCode,
    ValidateInput,
    Language,
    DDPFiletype,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="js_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "manifest.js",
            "account-creation-ip.js",
            "account-label.js",
            "account-suspension.js",
            "account-timezone.js",
            "account.js",
            "ad-engagements.js",
            "ad-free-article-visits.js",
            "ad-impressions.js",
            "ad-mobile-conversions-attributed.js",
            "ad-mobile-conversions-unattributed.js",
            "ad-online-conversions-attributed.js",
            "ad-online-conversions-unattributed.js",
            "ageinfo.js",
            "app.js",
            "birdwatch-note-rating.js",
            "birdwatch-note-tombstone.js",
            "birdwatch-note.js",
            "block.js",
            "branch-links.js",
            "catalog-item.js",
            "commerce-catalog.js",
            "community-tweet.js",
            "connected-application.js",
            "contact.js",
            "deleted-tweet.js",
            "device-token.js",
            "direct-message-group-headers.js",
            "direct-message-headers.js",
            "direct-message-mute.js",
            "direct-messages-group.js",
            "direct-messages.js",
            "email-address-change.js",
            "follower.js",
            "following.js",
            "ip-audit.js",
            "like.js",
            "lists-created.js",
            "lists-member.js",
            "lists-subscribed.js",
            "moment.js",
            "mute.js",
            "ni-devices.js",
            "periscope-account-information.js",
            "periscope-ban-information.js",
            "periscope-broadcast-metadata.js",
            "periscope-comments-made-by-user.js",
            "periscope-expired-broadcasts.js",
            "periscope-followers.js",
            "periscope-profile-description.js",
            "personalization.js",
            "phone-number.js",
            "product-drop.js",
            "product-set.js",
            "professional-data.js",
            "profile.js",
            "protected-history.js",
            "reply-prompt.js",
            "saved-search.js",
            "screen-name-change.js",
            "shop-module.js",
            "shopify-account.js",
            "smartblock.js",
            "spaces-metadata.js",
            "sso.js",
            "tweets.js",
            "tweetdeck.js",
            "twitter-circle-member.js",
            "twitter-circle-tweet.js",
            "twitter-circle.js",
            "twitter-shop.js",
            "user-link-clicks.js",
            "verified.js",
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message=""),
    StatusCode(id=1, description="Not valid DDP", message=""),
    StatusCode(id=2, description="Bad zipfile", message=""),
]


def find_items(d: dict[Any, Any],  key_to_match: str) -> str:
    """
    d is a denested dict
    match all keys in d that contain key_to_match

    return the value beloning to that key that are the least nested
    In case of no match return empty string

    example:
    key_to_match = asd

    asd-asd-asd-asd-asd-asd: 1
    asd-asd: 2
    qwe: 3

    returns 2

    This function is needed because your_posts_1.json contains a wide variety of nestedness per post
    """
    out = ""
    pattern = r"{}".format(f"^.*{key_to_match}.*$")
    depth = math.inf

    try:
        for k, v in d.items():
            if re.match(pattern, k):
                depth_current_match = k.count("-")
                if depth_current_match < depth:
                    depth = depth_current_match
                    out = str(v)
    except Exception as e:
        logger.error("bork bork: %s", e)

    return out
            

def validate(zfile: Path) -> ValidateInput:
    """
    Validates the input of a Youtube zipfile
    """

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".js"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        
        if validation.infer_ddp_category(paths):
            validation.set_status_code_by_id(0)
        else:
            validation.set_status_code_by_id(1)

    except zipfile.BadZipFile:
        validation.set_status_code_by_id(2)

    return validation


def bytesio_to_listdict(bytes_to_read: io.BytesIO) -> list[dict[Any, Any]]:
    """
    Converts a io.BytesIO buffer containing a twitter.js file, to a list of dicts

    A list of dicts is the current structure of twitter.js files
    """

    out = []
    lines = []

    try:
        with io.TextIOWrapper(bytes_to_read, encoding="utf8") as f:
            lines = f.readlines()

        # change first line so its a valid json
        lines[0] = re.sub("^.*? = ", "", lines[0])

        # convert to a list of dicts
        out = json.loads("".join(lines))

    except json.decoder.JSONDecodeError as e:
        logger.error("The input buffer did not contain a valid JSON: %s", e)
    except IndexError as e:
        logger.error("No lines were read, could be empty input buffer: %s", e)
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    finally:
        return out



def following_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    following.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "following.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("following", {}).get("userLink", None)
            ))
        out = pd.DataFrame(datapoints, columns=["Link to user"])
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out



def like_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    like.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "like.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("like", {}).get("tweetId", None),
                item.get("like", {}).get("fullText", None)
            ))
        out = pd.DataFrame(datapoints, columns=["Tweet Id", "Tweet"])
        out["Tweet Id"] = "https://twitter.com/a/status/" + out["Tweet Id"]
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out



def ad_engagements_to_df(twitter_zip: str) -> pd.DataFrame:
    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "ad-engagements.js")
    ld = bytesio_to_listdict(b)

    # engagement attributes left out!! CHECK
    try:
        for item in ld:
            denested_dict = helpers.dict_denester(item)
            datapoints.append((
                find_items(denested_dict, 'advertiserInfo-advertiserName'),
                find_items(denested_dict, 'impressionAttributes-impressionTime')
            ))

        df = pd.DataFrame(datapoints, columns=[
            "Advertiser name", 
            "Date"
            #"Engagement attributes",
        ])

        if not df.empty:
            grouped = df.groupby('Advertiser name').agg({'Date': ['count', 'min', 'max']})
            grouped = grouped.reset_index()
            grouped.columns = ["Advertiser name", "Number of views", "Earliest view", "Latest view"]
            out = grouped
            out = out.sort_values(by="Number of views", ascending=False)

    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def find_all(d: dict[Any, Any],  key_to_match: str) -> list[str]:
    """
    d is a denested dict
    """
    out = []
    pattern = r"{}".format(f"^.*{key_to_match}.*$")

    try:
        for k, v in d.items():
            if re.match(pattern, k):
                out.append(v)
    except Exception as e:
        logger.error("bork bork: %s", e)

    return out


def mentions_to_df(twitter_zip: str) -> pd.DataFrame:
    out = pd.DataFrame()

    # First look for tweet.js, then tweets.js
    b = unzipddp.extract_file_from_zip(twitter_zip, "tweet.js")
    ld = bytesio_to_listdict(b)
    if len(ld) == 0:
        b = unzipddp.extract_file_from_zip(twitter_zip, "tweets.js")
        ld = bytesio_to_listdict(b)

    try:
        denested_dict = helpers.dict_denester(ld)
        all_usermentions_screen_names = find_all(denested_dict, "user_mentions-[0-9]+-screen_name")
        out = pd.DataFrame(all_usermentions_screen_names, columns=["Screen name"])\
            .groupby("Screen name")\
            .size()\
            .reset_index(name="Number of mentions")\
            .sort_values("Number of mentions", ascending=False)\
            .reset_index(drop=True)

    except Exception as e:
        logger.error("Exception was caught: %s", e)

    print(out)

    return out


def replies_to_df(twitter_zip: str) ->pd.DataFrame:
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "tweet.js")
    ld = bytesio_to_listdict(b)
    if len(ld) == 0:
        b = unzipddp.extract_file_from_zip(twitter_zip, "tweets.js")
        ld = bytesio_to_listdict(b)

    try:
        denested_dict = helpers.dict_denester(ld)
        all_replies = find_all(denested_dict, "in_reply_to_screen_name")
        out = pd.DataFrame(all_replies, columns=["Screen name"])\
            .groupby("Screen name")\
            .size()\
            .reset_index(name="Number of replies")\
            .sort_values("Number of replies", ascending=False)\
            .reset_index(drop=True)


    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out



def tweets_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    tweets.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "tweet.js")
    ld = bytesio_to_listdict(b)
    if len(ld) == 0:
        b = unzipddp.extract_file_from_zip(twitter_zip, "tweets.js")
        ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("tweet", {}).get("created_at", None),
                item.get("tweet", {}).get("full_text", None),
                str(item.get("tweet", {}).get("retweeted", ""))
            ))
        out = pd.DataFrame(datapoints, columns=["Date", "Tweet", "Retweeted"])
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def user_link_clicks_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    user-link-clicks.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "user-link-clicks.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("userInteractionsData", {}).get("linkClick", {}).get("timeStampOfInteraction", None),
                item.get("userInteractionsData", {}).get("linkClick", {}).get("finalUrl", None),
                item.get("userInteractionsData", {}).get("linkClick", {}).get("tweetId", None),
            ))
        out = pd.DataFrame(datapoints, columns=["Date", "Url", "Tweet Id"])
        out["Tweet Id"] = "https://twitter.com/a/status/" + out["Tweet Id"]
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def block_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    block.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "block.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("blocking", {}).get("userLink", "")
            ))
        out = pd.DataFrame(datapoints, columns=["Blocked users"])
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def mute_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    mute.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = unzipddp.extract_file_from_zip(twitter_zip, "mute.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("muting", {}).get("userLink", "")
            ))
        out = pd.DataFrame(datapoints, columns=["Muted users"])
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out
