import logging
import json
import io

import pandas as pd

import port.api.props as props
import port.helpers as helpers
import port.validate as validate
import port.twitter as twitter
import port.facebook as facebook
import port.instagram as instagram

from port.api.commands import (CommandSystemDonate, CommandUIRender)

LOG_STREAM = io.StringIO()

logging.basicConfig(
    stream=LOG_STREAM,
    level=logging.INFO,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")


def process(session_id):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platforms = [
        ("Instagram", extract_instagram, instagram.validate),
        ("Facebook", extract_facebook, facebook.validate),
        ("X", extract_twitter, twitter.validate),
    ]

    # For each platform
    # 1. Prompt file extraction loop
    # 2. In case of succes render data on screen
    for platform in platforms:
        platform_name, extraction_fun, validation_fun = platform

        table_list = None

        # Prompt file extraction loop
        while True:
            LOGGER.info("Prompt for file for %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")

            # Render the propmt file page
            promptFile = prompt_file("application/zip, text/plain, application/json", platform_name)
            file_result = yield render_donation_page(platform_name, promptFile)

            if file_result.__type__ == "PayloadString":
                validation = validation_fun(file_result.value)

                # DDP is recognized: Status code zero
                if validation.status_code.id == 0: 
                    LOGGER.info("Payload for %s", platform_name)
                    yield donate_logs(f"{session_id}-tracking")

                    table_list = extraction_fun(file_result.value, validation)
                    break

                # DDP is not recognized: Different status code
                if validation.status_code.id != 0: 
                    LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                    yield donate_logs(f"{session_id}-tracking")
                    retry_result = yield render_donation_page(platform_name, retry_confirmation(platform_name))

                    if retry_result.__type__ == "PayloadTrue":
                        continue
                    else:
                        LOGGER.info("Skipped during retry %s", platform_name)
                        yield donate_logs(f"{session_id}-tracking")
                        break
            else:
                LOGGER.info("Skipped %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                break

        # Render data on screen
        if table_list is not None:
            LOGGER.info("Prompt consent; %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")

            # Check if extract something got extracted
            if len(table_list) == 0:
                table_list.append(create_empty_table(platform_name))

            prompt = assemble_tables_into_form(table_list)
            consent_result = yield render_donation_page(platform_name, prompt)

            if consent_result.__type__ == "PayloadJSON":
                LOGGER.info("Data donated; %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                yield donate(platform_name, consent_result.value)
            else:
                LOGGER.info("Skipped ater reviewing consent: %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
        print("CHECK")

    yield render_end_page()



##################################################################

def assemble_tables_into_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    return props.PropsUIPromptConsentForm(table_list, [])


def create_consent_form_tables(unique_table_id: str, title: props.Translatable, df: pd.DataFrame, vis = None) -> list[props.PropsUIPromptConsentFormTable]:

    out = []
    table = props.PropsUIPromptConsentFormTable(unique_table_id, title, df, visualizations=vis)
    out.append(table)

    return out


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def create_empty_table(platform_name: str) -> props.PropsUIPromptConsentFormTable:
    """
    Show something in case no data was extracted
    """
    title = props.Translatable({
       "en": "Er ging niks mis, maar we konden niks vinden",
       "nl": "Er ging niks mis, maar we konden niks vinden"
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table


##################################################################
# Extraction functions


def extract_twitter(twitter_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = twitter.like_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Your liked Tweets", "nl": "Your liked Tweets:"})
        tables = create_consent_form_tables("twitter_like", table_title, df) 
        tables_to_render.extend(tables)

    #df = twitter.following_to_df(twitter_zip)
    #if not df.empty:
    #    table_title =  props.Translatable( { "en": "Accounts you follow according to Twitter", "nl": "Profielen door jou gevold volgens Twitter:" })
    #    tables = create_consent_form_tables("twitter_following", table_title, df) 
    #    tables_to_render.extend(tables)

    df = twitter.ad_engagements_to_df(twitter_zip)
    if not df.empty:
        wordcloud = { 
            "title": {
                "nl":"Authors of ads weighted by view count", 
                "en":"Authors of ads weighted by view count"
            },
            "type": "wordcloud",
            "textColumn": "Advertiser name",
            "valueColumn":  "Number of views",
            "tokenize": False,
        }
        table_title = props.Translatable({ "en": "Your engagement with ads", "nl": "Your engagement with ads:"})
        tables = create_consent_form_tables("twitter_ad_engagements", table_title, df, [wordcloud]) 
        tables_to_render.extend(tables)

    df = twitter.replies_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable( { "en": "Accounts of tweets you replied to", "nl": "Accounts of tweets you replied to", })
        tables = create_consent_form_tables("twitter_replies", table_title, df) 
        tables_to_render.extend(tables)

    df = twitter.mentions_to_df(twitter_zip)
    if not df.empty:
        table_title = props.Translatable({ "en": "Accounts you mentioned in your Tweets", "nl": "Accounts you mentioned in your Tweets", })
        tables = create_consent_form_tables("twitter_mentions", table_title, df) 
        tables_to_render.extend(tables)


    return tables_to_render



def extract_facebook(facebook_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = facebook.who_you_follow_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Who you follow on Facebook", "nl": "Who you follow on Facebook"})
        tables = create_consent_form_tables("facebook_who_you_follow", table_title, df) 
        tables_to_render.extend(tables)
        
    df = facebook.recently_viewed_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Recently viewed items on Facebook", "nl": "Items recently viewd on Facebook"})
        tables = create_consent_form_tables("facebook_recently_viewed", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.likes_and_reactions_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Your likes and reactions on Facebook", "nl": "Your likes and reactions on Facebook"})
        tables = create_consent_form_tables("facebook_likes_and_reactions", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.comments_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Liked comments on Facebook", "nl": "Liked comments on Facebook"})
        tables = create_consent_form_tables("facebook_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = facebook.your_saved_items(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Your saved items on Facebook", "nl": "Your your saved items on Facebook"})
        tables = create_consent_form_tables("facebook_your_saved_items", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render



def extract_instagram(instagram_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = instagram.following_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Following on Instagram", "nl": "Following on Instagram"})
        tables = create_consent_form_tables("instagram_following", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.ads_viewed_to_df(instagram_zip)
    if not df.empty:
        wordcloud = { 
            "title": {
                "nl":"Authors of ads weighted by view count", 
                "en":"Authors of ads weighted by view count"
            },
            "type": "wordcloud",
            "textColumn": "Author of ad",
            "valueColumn":  "Number of views",
            "tokenize": False,
        }
        table_title = props.Translatable( { "en": "Ads viewed on Instagram", "nl": "Advertenties gezien op Instagram", })
        tables = create_consent_form_tables("instagram_ads_viewed", table_title, df, [wordcloud]) 
        tables_to_render.extend(tables)

    df = instagram.posts_viewed_to_df(instagram_zip)
    if not df.empty:
        wordcloud = { 
            "title": {
                "nl":"Authors of posts weighted by view count", 
                "en":"Authors of posts weighted by view count"
            },
            "type": "wordcloud",
            "textColumn": "Author of post",
            "valueColumn":  "Number of views",
            "tokenize": False,
        }
        table_title = props.Translatable( { "en": "Posts viewed on Instagram", "nl": "Posts viewed on Instagram", })
        tables = create_consent_form_tables("instagram_posts_viewed", table_title, df, [wordcloud]) 
        tables_to_render.extend(tables)

    df = instagram.videos_watched_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable( { "en": "Videos watched on Instagram", "nl": "Video's bekeken op Instagram", })
        tables = create_consent_form_tables("instagram_videos_watched", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.post_comments_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable( { "en": "Comments on posts on Instagram", "nl": "Post comments op Instagram", })
        tables = create_consent_form_tables("instagram_post_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.reels_comments_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable( { "en": "Comments on reels on Instagram", "nl": "Reels comments op Instagram", })
        tables = create_consent_form_tables("instagram_reels_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.liked_posts_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable( { "en": "Liked posts on Instagram", "nl": "Liked posts op Instagram", })
        tables = create_consent_form_tables("instagram_liked_posts", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.story_likes_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable( { "en": "Liked stories on Instagram", "nl": "Liked stories op Instagram", })
        tables = create_consent_form_tables("instagram_liked_comments", table_title, df) 
        tables_to_render.extend(tables)

    df = instagram.saved_posts_to_df(instagram_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Saved posts on Instagram", "nl": "Saved posts on Instagram"})
        tables = create_consent_form_tables("instagram_saved_posts", table_title, df) 
        tables_to_render.extend(tables)

    return tables_to_render



##########################################
# Functions provided by Eyra did not change

def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_donation_page(platform, body):
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform}))

    footer = props.PropsUIFooter()
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(extensions, platform):
    description = props.Translatable(
        {
            "en": f"Please choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a file from {platform}.",
            "nl": f"Please choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a file from {platform}.",
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)
