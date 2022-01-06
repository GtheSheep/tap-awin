"""Stream type classes for tap-awin-publisher."""

import datetime
from urllib.parse import urlparse
from urllib.parse import parse_qs
from typing import Any, Optional

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_awin_publisher.client import AwinPublisherStream

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


class AccountsStream(AwinPublisherStream):
    name = "accounts"
    path = "/accounts"
    primary_keys = ["accountId"]
    replication_key = None
    records_jsonpath = "$.accounts[*]"
    schema = th.PropertiesList(
        th.Property(
            "accountId",
            th.IntegerType,
            description="The Account's ID"
        ),
        th.Property(
            "accountName",
            th.StringType,
            description="Given name for the account"
        ),
        th.Property(
            "accountType",
            th.StringType,
            description="Type of account"
        ),
        th.Property(
            "userRole",
            th.StringType,
            description="Role granted to the user querying the account"
        ),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "account_id": record["accountId"]
        }


class TransactionsStream(AwinPublisherStream):
    name = "transactions"
    parent_stream_type = AccountsStream
    ignore_parent_replication_keys = True
    path = "/advertisers/{account_id}/transactions/"
    primary_keys = ["id"]
    replication_key = "transactionDate"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("url", th.StringType),
        th.Property("advertiserId", th.IntegerType),
        th.Property("publisherId", th.IntegerType),
        th.Property("commissionSharingPublisherId", th.IntegerType),
        th.Property("commissionSharingSelectedRatePublisherId", th.IntegerType),
        th.Property("siteName", th.StringType),
        th.Property("commissionStatus", th.StringType),
        th.Property("commissionAmount", th.ObjectType(
            th.Property("amount", th.NumberType),
            th.Property("currency", th.StringType),
        )),
        th.Property("saleAmount", th.ObjectType(
            th.Property("amount", th.NumberType),
            th.Property("currency", th.StringType),
        )),
        th.Property("ipHash", th.IntegerType),
        th.Property("customerCountry", th.StringType),
        th.Property("clickRefs", th.ObjectType(
            th.Property("clickRefs", th.StringType),
        )),
        th.Property("clickDate", th.DateTimeType),
        th.Property("transactionDate", th.DateTimeType),
        th.Property("validationDate", th.DateTimeType),
        th.Property("type", th.StringType),
        th.Property("declineReason", th.StringType),
        th.Property("voucherCodeUsed", th.BooleanType),
        th.Property("voucherCode", th.StringType),
        th.Property("lapseTime", th.IntegerType),
        th.Property("amended", th.BooleanType),
        th.Property("amendReason", th.StringType),
        th.Property("oldSaleAmount", th.ObjectType(
            th.Property("amount", th.NumberType),
            th.Property("currency", th.StringType),
        )),
        th.Property("oldCommissionAmount", th.ObjectType(
            th.Property("amount", th.NumberType),
            th.Property("currency", th.StringType),
        )),
        th.Property("clickDevice", th.StringType),
        th.Property("transactionDevice", th.StringType),
        th.Property("publisherUrl", th.StringType),
        th.Property("advertiserCountry", th.StringType),
        th.Property("orderRef", th.StringType),
        th.Property("customParameters", th.ArrayType(
            th.ObjectType(
                th.Property("key", th.StringType),
                th.Property("value", th.StringType),
            )
        )),
        th.Property("transactionParts", th.ArrayType(
            th.ObjectType(
                th.Property("advertiserCost", th.ObjectType(
                    th.Property("amount", th.NumberType),
                    th.Property("currency", th.StringType),
                )),
                th.Property("amount", th.NumberType),
                th.Property("commissionAmount", th.NumberType),
                th.Property("commissionGroupCode", th.StringType),
                th.Property("commissionGroupId", th.IntegerType),
                th.Property("commissionGroupName", th.StringType),
                th.Property("trackedParts", th.ArrayType(
                    th.ObjectType(
                        th.Property("amount", th.NumberType),
                        th.Property("code", th.StringType),
                        th.Property("currency", th.StringType),
                    )
                ))
            )
        )),
        th.Property("paidToPublisher", th.BooleanType),
        th.Property("paymentId", th.IntegerType),
        th.Property("transactionQueryId", th.IntegerType),
        th.Property("originalSaleAmount", th.NumberType),
        th.Property("advertiserCost", th.ObjectType(
            th.Property("amount", th.NumberType),
            th.Property("currency", th.StringType),
        )),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        if isinstance(next_page_token, datetime.datetime):
            start_date = next_page_token
        else:
            start_date = self.get_starting_timestamp(context) - datetime.timedelta(days=self.config.get("lookback_days"))
        yesterday = datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=1)
        end_date = min(start_date + datetime.timedelta(days=1), yesterday)
        params = {
            'startDate': datetime.datetime.strftime(
                start_date.replace(hour=0, minute=0, second=0, microsecond=0),
                TIMESTAMP_FORMAT
            ),
            'endDate': datetime.datetime.strftime(
                end_date.replace(hour=0, minute=0, second=0, microsecond=0),
                TIMESTAMP_FORMAT
            ),
            'timezone': self.config.get("timezone"),
            'dateType': 'transaction',
            'accessToken': self.config.get("api_token")
        }
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        elif response.headers.get("X-Next-Page", None):
            next_page_token = response.headers.get("X-Next-Page", None)
        else:
            end_date = datetime.datetime.strptime(
                parse_qs(urlparse(response.request.url).query)['endDate'][0],
                TIMESTAMP_FORMAT
            )
            if end_date.date() < datetime.date.today():
                next_page_token = end_date
            else:
                next_page_token = None
        return next_page_token
