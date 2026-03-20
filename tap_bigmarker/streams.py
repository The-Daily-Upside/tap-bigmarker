"""Stream type classes for tap-bigmarker."""

from pathlib import Path
from typing import Any, Optional, Iterable, Dict


from tap_bigmarker.client import BigMarkerStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
RAW_SCHEMA_PATH = SCHEMAS_DIR / "bigmarker_raw.json"


def _source_id(record: dict, *keys: str) -> Optional[Any]:
    """Get an identifier from the original API payload, not wrapped hash id."""
    if not isinstance(record, dict):
        return None

    raw = record.get("raw")
    if isinstance(raw, dict):
        for key in keys:
            val = raw.get(key)
            if val not in (None, ""):
                return val

    # Fall back only to explicit non-wrapper ids. Never use wrapped `id`.
    for key in keys:
        if key == "id":
            continue
        val = record.get(key)
        if val not in (None, ""):
            return val
    return None


class ChannelsStream(BigMarkerStream):
    name = "channels"
    path = "/channels"
    records_jsonpath = "$.channels[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    has_pagination = False

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"channel_id": _source_id(record, "channel_id", "id")}


class ChannelsSubscribersStream(BigMarkerStream):
    name = "channels_subscribers"
    path = "/channels/{channel_id}/subscribers"
    records_jsonpath = "$.subscribers[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ChannelsStream
    ignore_parent_replication_keys = True
    has_pagination = False


class ChannelsAdminsStream(BigMarkerStream):
    name = "channels_admins"
    path = "/channel_admins/{channel_id}"
    records_jsonpath = "$[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ChannelsStream
    ignore_parent_replication_keys = True
    has_pagination = False


class ConferencesStream(BigMarkerStream):
    name = "conferences"
    path = "/conferences/search/"
    # Singer SDK 0.48+ uses `http_method` (not `rest_method`).
    http_method = "POST"
    records_jsonpath = "$.conferences[*]"
    primary_keys = ["id"]
    # Avoid Singer incremental bookmark bookkeeping (can produce non-JSON-serializable
    # state values like `Decimal` in some loader versions).
    # We still implement incremental behavior via `state['last_date']` below.
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    per_page = 500

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"conference_id": _source_id(record, "conference_id", "id")}
    
    
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        # Full historical ingestion: always ask for all conferences since the
        # BigMarker epoch boundary. This avoids missing older conferences due
        # to rolling bookmarks/state.
        params: dict = {}
        if next_page_token:
            params[self.page_key] = next_page_token
        if self.per_page:
            params["per_page"] = self.per_page
        params["start_time"] = 0
        return params
    
class ConferencesHandoutsStream(BigMarkerStream):
    name = "conferences_handouts"
    path = "/conferences/{conference_id}/handouts"
    records_jsonpath = "$[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False
    tolerated_http_errors = [401, 404]

class ConferencesSurveysStream(BigMarkerStream):
    name = "conferences_surveys"
    path = "/conferences/survey/{conference_id}"
    records_jsonpath = "$.survey[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False
    tolerated_http_errors = [401, 404]

class ConferencesPresentersStream(BigMarkerStream):
    name = "conferences_presenters"
    path = "/conferences/{conference_id}/presenters"
    records_jsonpath = "$.presenters[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False

class ConferencesAttendeesStream(BigMarkerStream):
    name = "conferences_attendees"
    path = "/conferences/{conference_id}/attendees"
    page_key = "current_page"
    records_jsonpath = "$.attendees[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    per_page = 500

class ConferencesRegistrantsStream(BigMarkerStream):
    name = "conferences_registrants"
    path = "/conferences/registrations_with_fields/{conference_id}"
    records_jsonpath = "$.registrations[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    # This endpoint is frequently throttled in production; use smaller pages and
    # skip on persistent 429 so other webinar streams can continue.
    per_page = 100
    tolerated_http_errors = [401, 404, 429]

class ConferencesRegistrationsLiveStream(BigMarkerStream):
    name = "conferences_registrations_live"
    path = "/reporting/conferences/registrations/{conference_id}"
    page_key = "current_page"
    records_jsonpath = "$.registrations[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    per_page = 500

class ConferencesAttendeesLiveStream(BigMarkerStream):
    name = "conferences_attendees_live"
    path = "/reporting/conferences/live_attendees/{conference_id}"
    page_key = "current_page"
    records_jsonpath = "$.attendees[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    per_page = 500


class ConferencesRegistrationsNoShowsStream(BigMarkerStream):
    name = "conferences_registrations_no_shows"
    path = "/reporting/conferences/no_shows/{conference_id}"
    page_key = "current_page"
    records_jsonpath = "$.registrations[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    per_page = 500
    

class ConferencesRegistrationsQaStream(BigMarkerStream):
    name = "conferences_registrations_qa"
    path = "/reporting/conferences/q_and_a_transcript/{conference_id}"
    page_key = "current_page"
    records_jsonpath = "$.q_and_a[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    per_page = 500


class ConferencesAttendeesOnDemandStream(BigMarkerStream):
    name = "conferences_attendees_on_demand"
    path = "/reporting/conferences/on_demand_attendees/{conference_id}"
    page_key = "current_page"
    records_jsonpath = "$.attendees[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    per_page = 500


class ChannelsConferencesStream(BigMarkerStream):
    name = "channels_conferences"
    path = "/channels/{channel_id}/conferences"
    records_jsonpath = "$.conferences[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ChannelsStream
    ignore_parent_replication_keys = True
    page_key = "current_page"
    per_page = 500


class ChannelsConferencesStatsStream(BigMarkerStream):
    name = "channels_conferences_stats"
    path = "/channels/{channel_id}/conferences/stats"
    records_jsonpath = "$.conferences[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ChannelsStream
    ignore_parent_replication_keys = True
    page_key = "current_page"
    per_page = 500


class ConferenceDetailStream(BigMarkerStream):
    name = "conference_detail"
    path = "/conferences/{conference_id}"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False


class ConferenceCustomFieldsStream(BigMarkerStream):
    name = "conference_custom_fields"
    path = "/conferences/custom_fields/{conference_id}"
    records_jsonpath = "$[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False


class ConferenceAssociatedSessionsStream(BigMarkerStream):
    name = "conference_associated_sessions"
    path = "/conferences/get_associated_sessions/{conference_id}"
    records_jsonpath = "$.conferences[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferenceRecurringChildrenStream(BigMarkerStream):
    name = "conference_recurring_children"
    path = "/conferences/recurring/{conference_id}"
    records_jsonpath = "$[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False
    tolerated_http_errors = [401, 404]


class _ConferenceDownloadBmidRequiredStream(BigMarkerStream):
    """Base class for endpoints that require an admin download `bmid`."""

    has_pagination = False

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        if not self.config.get("download_bmid"):
            self.logger.info(
                "Skipping %s: missing `download_bmid` in tap config",
                self.name,
            )
            return []
        yield from (self.post_process(r, context) for r in self.request_records(context))

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["bmid"] = self.config.get("download_bmid")
        return params


class ConferenceRecordingStatusStream(BigMarkerStream):
    name = "conference_recording_status"
    path = "/conferences/{conference_id}/recording"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False
    tolerated_http_errors = [401, 404]


class ConferenceRecordingViewsStream(BigMarkerStream):
    name = "conference_recording_views"
    path = "/conferences/{conference_id}/recording_views"
    records_jsonpath = "$.recording_views[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = True
    page_key = "current_page"
    per_page = 500
    tolerated_http_errors = [401, 404]


class ConferenceAdminUrlStream(_ConferenceDownloadBmidRequiredStream):
    name = "conference_admin_url"
    path = "/conferences/{conference_id}/admin_url"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferenceTranscriptUrlStream(_ConferenceDownloadBmidRequiredStream):
    name = "conference_transcript_url"
    path = "/conferences/{conference_id}/transcript"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferenceStatsUrlStream(_ConferenceDownloadBmidRequiredStream):
    name = "conference_stats_url"
    path = "/conferences/{conference_id}/stats"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferenceRegistrationsSimpleStream(BigMarkerStream):
    name = "conference_registrations"
    path = "/conferences/registrations/{conference_id}"
    page_key = "current_page"
    per_page = 500
    records_jsonpath = "$.registrations[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferenceCheckedInRegistrantsStream(BigMarkerStream):
    name = "conference_checked_in_registrants"
    path = "/conferences/checked_in_registrants/{conference_id}"
    page_key = "current_page"
    per_page = 500
    records_jsonpath = "$.registrations[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferenceAutomationsTimelineStream(BigMarkerStream):
    name = "conference_automations_timeline"
    path = "/automations/{conference_id}"
    page_key = "current_page"
    per_page = 500
    records_jsonpath = "$.automations[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferenceChatTranscriptStream(BigMarkerStream):
    name = "conference_chat_transcript"
    path = "/reporting/conferences/chat_transcript/{conference_id}"
    page_key = "current_page"
    per_page = 500
    records_jsonpath = "$.chats[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferencePollTranscriptStream(BigMarkerStream):
    name = "conference_poll_transcript"
    path = "/reporting/conferences/poll_transcript/{conference_id}"
    page_key = "current_page"
    per_page = 500
    records_jsonpath = "$.polls[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferenceOfferTranscriptStream(BigMarkerStream):
    name = "conference_offer_transcript"
    path = "/reporting/conferences/offer_transcript/{conference_id}"
    page_key = "current_page"
    per_page = 500
    records_jsonpath = "$.offers[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferenceEventSummaryStream(BigMarkerStream):
    name = "conference_event_summary"
    path = "/reporting/conferences/event_summary/{conference_id}"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False


class ConferenceEnterExitLogStream(BigMarkerStream):
    name = "conference_enter_exit_log"
    path = "/reporting/conferences/enter_exit_log/{conference_id}"
    page_key = "current_page"
    per_page = 500
    records_jsonpath = "$.activities[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferencePresenterEngagementStream(BigMarkerStream):
    name = "conference_presenter_engagement"
    path = "/reporting/conferences/presenter_engagement/{conference_id}"
    page_key = "current_page"
    per_page = 500
    records_jsonpath = "$.activities[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class ConferenceAttendanceMonitorDataStream(BigMarkerStream):
    name = "conference_attendance_monitor_data"
    path = "/reporting/conferences/attendance_monitor_data/{conference_id}"
    page_key = "current_page"
    per_page = 500
    records_jsonpath = "$.activities[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True


class CustomEmailSeedStream(BigMarkerStream):
    name = "custom_email_seed"
    path = "/custom_emails/seed"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    has_pagination = False

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        custom_email_id = self.config.get("custom_email_id")
        if not custom_email_id:
            return []
        # Yield once to create a stable child context.
        yield self.post_process({"custom_email_id": custom_email_id}, context or {})

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"custom_email_id": record.get("custom_email_id")}


class CustomEmailStatisticsStream(BigMarkerStream):
    name = "custom_email_statistics_data"
    path = "/custom_emails/email_statistics_data"
    records_jsonpath = "$.data[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = CustomEmailSeedStream
    ignore_parent_replication_keys = True
    has_pagination = False

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        if context and context.get("custom_email_id") is not None:
            params["custom_email_id"] = context["custom_email_id"]
        return params


class CustomEmailSuppressionsStream(BigMarkerStream):
    name = "custom_email_suppressions"
    path = "/custom_emails/{custom_email_id}/suppressions"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = CustomEmailSeedStream
    ignore_parent_replication_keys = True
    has_pagination = False


class CustomEmailAnalyticsStream(BigMarkerStream):
    name = "custom_email_analytics"
    path = "/custom_emails/{custom_email_id}/email_analytics"
    records_jsonpath = "$.xxx[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = CustomEmailSeedStream
    ignore_parent_replication_keys = True
    page_key = "current_page"
    per_page = 500

    def parse_response(self, response):
        payload = response.json()
        if isinstance(payload, dict):
            # Docs show the analytics array under a dynamic key (example uses `xxx`).
            # Prefer any list-valued key that is not pagination metadata.
            meta_keys = {"per_page", "current_page", "total_entries", "total_pages"}
            # Prefer common keys first if present.
            for preferred_key in ("data", "analytics", "email_analytics", "items", "records", "xxx"):
                preferred_val = payload.get(preferred_key)
                if isinstance(preferred_val, list):
                    for row in preferred_val:
                        yield row
                    return
            for k, v in payload.items():
                if k not in meta_keys and isinstance(v, list):
                    for row in v:
                        yield row
                    return
        yield from super().parse_response(response)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        # Optional report filters for email analytics endpoints.
        for cfg_key in ("custom_email_action_type", "custom_email_date_start", "custom_email_date_end"):
            cfg_val = self.config.get(cfg_key)
            if cfg_val not in (None, ""):
                param_key = cfg_key.replace("custom_email_", "")
                params[param_key] = cfg_val
        return params


class ConferenceHandoutDownloadDataStream(BigMarkerStream):
    name = "conference_handout_download_data"
    path = "/conferences/handout_download_data/{conference_id}"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False


class ConferenceDialInListStream(BigMarkerStream):
    name = "conference_dial_in_list"
    path = "/conferences/dial_in_list/{conference_id}"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False


class ConferenceIntegrationSettingsStream(BigMarkerStream):
    name = "conference_integration_settings"
    path = "/conferences/get_integration_settings/{conference_id}"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False
    tolerated_http_errors = [401, 404, 429]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        # This endpoint is not paginated; avoid sending default per_page/page params.
        params: Dict[str, Any] = {}
        params["integration_type"] = self.config.get("integration_type") or "salesforce"
        return params


class ConferenceActivityLogsStream(BigMarkerStream):
    name = "conference_activity_logs"
    path = "/conferences/{conference_id}/activity_logs"
    records_jsonpath = "$.activity_logs[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    has_pagination = False


class ChannelRegistrationsStream(BigMarkerStream):
    name = "channel_registrations"
    path = "/conferences/registrations/channel/{channel_id}"
    records_jsonpath = "$.registrations[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ChannelsStream
    ignore_parent_replication_keys = True
    page_key = "current_page"
    per_page = 500


class ChannelRegisteredConferencesStream(BigMarkerStream):
    name = "channel_registered_conferences"
    path = "/channels/{channel_id}/list_registered_conferences"
    records_jsonpath = "$.conferences[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ChannelsStream
    ignore_parent_replication_keys = True
    page_key = "page"
    per_page = 500

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        if not self.config.get("registered_conferences_email"):
            self.logger.info(
                "Skipping %s: missing `registered_conferences_email` in tap config",
                self.name,
            )
            return []
        yield from super().get_records(context)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["email"] = self.config.get("registered_conferences_email")
        return params


class ChannelBlockListStream(BigMarkerStream):
    name = "channel_block_list"
    path = "/channels/block_list/{channel_id}"
    records_jsonpath = "$.list_data[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ChannelsStream
    ignore_parent_replication_keys = True
    page_key = "current_page"
    per_page = 500

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["list_type"] = self.config.get("channel_block_list_type") or "block_list"
        return params


class ConferenceRegistrationBlockListStream(BigMarkerStream):
    name = "conference_registration_block_list"
    path = "/registration_block_list/{conference_id}"
    http_method = "POST"
    records_jsonpath = "$.list[*]"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = ConferencesStream
    ignore_parent_replication_keys = True
    page_key = "current_page"
    per_page = 500
    # BigMarker can return persistent 500s for specific conferences on this endpoint.
    # Treat these as tolerated so one bad conference does not stall the entire sync.
    tolerated_http_errors = [401, 500]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        # This endpoint is POST-based but supports pagination/list_type filters.
        # Build parameters explicitly instead of relying on GET-only base logic.
        params: Dict[str, Any] = {}
        if next_page_token:
            params[self.page_key] = next_page_token
        if self.per_page:
            params["per_page"] = self.per_page
        params["list_type"] = self.config.get("registration_block_list_type") or "block_list"
        return params


class MeetingSpaceSeedStream(BigMarkerStream):
    name = "meeting_space_seed"
    path = "/meeting_spaces/seed"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    has_pagination = False

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        meeting_space_id = self.config.get("meeting_space_id")
        if not meeting_space_id:
            return []
        yield self.post_process({"meeting_space_id": meeting_space_id}, context or {})

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"meeting_space_id": record.get("meeting_space_id")}


class MeetingSpaceDetailStream(BigMarkerStream):
    name = "meeting_space_detail"
    path = "/meeting_spaces/{meeting_space_id}"
    records_jsonpath = "$"
    primary_keys = ["id"]
    replication_method = "FULL_TABLE"
    schema_filepath = RAW_SCHEMA_PATH
    wrap_raw = True
    parent_stream_type = MeetingSpaceSeedStream
    ignore_parent_replication_keys = True
    has_pagination = False