# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import contextlib
import datetime
import io
import jwt
import logging
import requests
from flask import current_app, g, make_response, request, Response, render_template
from flask_appbuilder.api import expose, protect
from flask_babel import gettext as _
from marshmallow import ValidationError
from typing import Any, TYPE_CHECKING, Tuple

from superset import is_feature_enabled, security_manager
from superset.async_events.async_query_manager import AsyncQueryTokenException
from superset.charts.api import ChartRestApi
from superset.charts.client_processing import apply_client_processing
from superset.charts.data.query_context_cache_loader import QueryContextCacheLoader
from superset.charts.schemas import ChartDataQueryContextSchema
from superset.commands.chart.data.create_async_job_command import (
    CreateAsyncChartDataJobCommand,
)
from superset.commands.chart.data.get_data_command import ChartDataCommand
from superset.commands.chart.exceptions import (
    ChartDataCacheLoadError,
    ChartDataQueryFailedError,
)
from superset.common.chart_data import ChartDataResultFormat, ChartDataResultType
from superset.connectors.sqla.models import BaseDatasource
from superset.daos.exceptions import DatasourceNotFound
from superset.exceptions import QueryObjectValidationError
from superset.extensions import event_logger
from superset.models.sql_lab import Query
from superset.utils import json
from superset.utils.core import (
    create_zip,
    DatasourceType,
    get_user_id,
)
from superset.utils.decorators import logs_context
from superset.views.base import CsvResponse, generate_download_headers, XlsxResponse
from superset.views.base_api import statsd_metrics

if TYPE_CHECKING:
    from superset.common.query_context import QueryContext

logger = logging.getLogger(__name__)


class ChartDataRestApi(ChartRestApi):
    include_route_methods = {"get_data", "data", "data_from_cache"}

    @expose("/<int:pk>/data/", methods=("GET",))
    @protect()
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.data",
        log_to_statsd=False,
    )
    def get_data(self, pk: int) -> Response:
        """
        Take a chart ID and uses the query context stored when the chart was saved
        to return payload data response.
        ---
        get:
          summary: Return payload data response for a chart
          description: >-
            Takes a chart ID and uses the query context stored when the chart was saved
            to return payload data response.
          parameters:
          - in: path
            schema:
              type: integer
            name: pk
            description: The chart ID
          - in: query
            name: format
            description: The format in which the data should be returned
            schema:
              type: string
          - in: query
            name: type
            description: The type in which the data should be returned
            schema:
              type: string
          - in: query
            name: force
            description: Should the queries be forced to load from the source
            schema:
                type: boolean
          responses:
            200:
              description: Query result
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/ChartDataResponseSchema"
            202:
              description: Async job details
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/ChartDataAsyncResponseSchema"
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            500:
              $ref: '#/components/responses/500'
        """
        chart = self.datamodel.get(pk, self._base_filters)
        if not chart:
            return self.response_404()

        try:
            json_body = json.loads(chart.query_context)
        except (TypeError, json.JSONDecodeError):
            json_body = None

        if json_body is None:
            return self.response_400(
                message=_(
                    "Chart has no query context saved. Please save the chart again."
                )
            )

        # override saved query context
        json_body["result_format"] = request.args.get(
            "format", ChartDataResultFormat.JSON
        )
        json_body["result_type"] = request.args.get("type", ChartDataResultType.FULL)
        json_body["force"] = request.args.get("force")

        try:
            query_context = self._create_query_context_from_form(json_body)
            command = ChartDataCommand(query_context)
            command.validate()
        except DatasourceNotFound:
            return self.response_404()
        except QueryObjectValidationError as error:
            return self.response_400(message=error.message)
        except ValidationError as error:
            return self.response_400(
                message=_(
                    "Request is incorrect: %(error)s", error=error.normalized_messages()
                )
            )

        # TODO: support CSV, SQL query and other non-JSON types
        if (
            is_feature_enabled("GLOBAL_ASYNC_QUERIES")
            and query_context.result_format == ChartDataResultFormat.JSON
            and query_context.result_type == ChartDataResultType.FULL
        ):
            return self._run_async(json_body, command)

        try:
            form_data = json.loads(chart.params)
        except (TypeError, json.JSONDecodeError):
            form_data = {}

        return self._get_data_response(
            command=command, form_data=form_data, datasource=query_context.datasource, oid=''
        )

    @expose("/data", methods=("POST",))
    @protect()
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.data",
        log_to_statsd=False,
    )
    def data(self) -> Response:
        """
        Take a query context constructed in the client and return payload
        data response for the given query
        ---
        post:
          summary: Return payload data response for the given query
          description: >-
            Takes a query context constructed in the client and returns payload data
            response for the given query.
          requestBody:
            description: >-
              A query context consists of a datasource from which to fetch data
              and one or many query objects.
            required: true
            content:
              application/json:
                schema:
                  $ref: "#/components/schemas/ChartDataQueryContextSchema"
          responses:
            200:
              description: Query result
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/ChartDataResponseSchema"
            202:
              description: Async job details
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/ChartDataAsyncResponseSchema"
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            500:
              $ref: '#/components/responses/500'
        """
        json_body = None

        oid = ''
        bid = ''
        try:
            cookie_header = request.headers.get("Cookie", "")
            # Parse cookies into a dictionary
            cookies = {kv.split("=")[0]: "=".join(kv.split("=")[1:]) for kv in
                       cookie_header.split("; ") if "=" in kv}
            # Extract the 'sess' value
            sess_token = cookies.get("sess", None)
            decoded = jwt.decode(sess_token, options={"verify_signature": False})
            oid = decoded.get('oid')
            bid = decoded.get('b-id',"") or decoded.get('w-id')
        except Exception as e:
            #print("=====exception===", str(e))
            logger.error(f"=====exception==={e}")
        # print("=====INSIDE FUNC=1=====", str(datetime.datetime.now()))


        if request.is_json:
            json_body = request.json
        elif request.form.get("form_data"):
            # CSV export submits regular form data
            with contextlib.suppress(TypeError, json.JSONDecodeError):
                json_body = json.loads(request.form["form_data"])
        if json_body is None:
            return self.response_400(message=_("Request is not JSON"))

        try:
            query_context = self._create_query_context_from_form(json_body)
            try:
                if query_context.result_format in ChartDataResultFormat.table_like():
                    #print("=====INSIDE FUNC=2=====", str(datetime.datetime.now()))
                    logger.info(f"=====INSIDE FUNC=2==={datetime.datetime.now()}==")
            except Exception as e:
                print("====INSIDE FUNC=2=exception===", str(e))
            command = ChartDataCommand(query_context)
            command.validate()
        except DatasourceNotFound:
            return self.response_404()
        except QueryObjectValidationError as error:
            return self.response_400(message=error.message)
        except ValidationError as error:
            return self.response_400(
                message=_(
                    "Request is incorrect: %(error)s", error=error.normalized_messages()
                )
            )

        if query_context.result_format in ChartDataResultFormat.table_like():
            logger.info(f"=====INSIDE FUNC=3==={datetime.datetime.now()}==")

        # TODO: support CSV, SQL query and other non-JSON types
        # Always run async for download cases
        if (
            is_feature_enabled("GLOBAL_ASYNC_QUERIES")
            and query_context.result_format == ChartDataResultFormat.JSON
            and query_context.result_type == ChartDataResultType.FULL
        ):
            logger.info("======running async======")
            return self._run_async(json_body, command)

        form_data = json_body.get("form_data")

        if query_context.result_format in ChartDataResultFormat.download_like():
            # Eka special Async Download case
            # BnB API Call
            datasource = query_context.form_data.get('datasource')
            if datasource and datasource.split("__")[0]:
                slice_id = query_context.form_data.get('slice_id', '')
                dashboard_id = query_context.form_data.get('dashboardId', '')
                dataset_id = int(datasource.split("__")[0])
                # Call BNB API to Download

                url_params = form_data.get("url_params", {})
                start = url_params.get("start", "")
                end = url_params.get("end", "")
                oid = url_params.get("end", "")
                bid = url_params.get('b-id',"") or url_params.get('w-id')

                resp_json, status = self.start_async_download(dataset_id, slice_id, oid, bid, start, end)
                logger.info(f"=====response from self.start_async_download==={resp_json}===")

                if status:
                    html_content = render_template('superset/status_loader.html',
                                                   task_id=resp_json['task_id'],
                                                   email=resp_json['email'])
                    return Response(html_content, status=200, mimetype='text/html')
                    #return self.response(202, **resp_json)
                else:
                    # Should mean API has failed - that's how bnb async download API is written
                    err = resp_json.get("error", "Unknown Error")
                    return self.response(400, message=err)

        return self._get_data_response(
            command, form_data=form_data, datasource=query_context.datasource
        )

    def start_async_download(self, dataset_id, slice_id,oid, bid, start, end)  -> \
        tuple[Any, bool]:
        # {
        #     "dataset_id": "164",
        #     "slice_id": "3218",
        #     "user_sk": "50966",
        #     "oid": "161467756044203"
        # }
        url = "http://bnb.orbi.orbi/api/download_analytics/"
        payload = json.dumps({
            "dataset_id": dataset_id,
            "slice_id": slice_id,
            "user_sk": "",
            "business_sk": "",
            "dashboard_id": dashboard_id,
            "oid": oid,
            "bid": bid,
            "start": start,
            "end": end
        })
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
        response = requests.post(url, headers=headers, data=payload)
        logger.info(f"=====response from BNB API==={response.text}===")

        if str(response.status_code).startswith("2"):
            return response.json(), True
        else:
            logger.error(f"Error calling Async Download API: {response.text}")
            return response.json(), False



    @expose("/data/<cache_key>", methods=("GET",))
    @protect()
    @statsd_metrics
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}"
        f".data_from_cache",
        log_to_statsd=False,
    )
    def data_from_cache(self, cache_key: str) -> Response:
        """
        Take a query context cache key and return payload
        data response for the given query.
        ---
        get:
          summary: Return payload data response for the given query
          description: >-
            Takes a query context cache key and returns payload data
            response for the given query.
          parameters:
          - in: path
            schema:
              type: string
            name: cache_key
          responses:
            200:
              description: Query result
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/ChartDataResponseSchema"
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            cached_data = self._load_query_context_form_from_cache(cache_key)
            # Set form_data in Flask Global as it is used as a fallback
            # for async queries with jinja context
            g.form_data = cached_data
            query_context = self._create_query_context_from_form(cached_data)
            command = ChartDataCommand(query_context)
            command.validate()
        except ChartDataCacheLoadError:
            return self.response_404()
        except ValidationError as error:
            return self.response_400(
                message=_("Request is incorrect: %(error)s", error=error.messages)
            )

        return self._get_data_response(command, True)

    def _run_async(
        self, form_data: dict[str, Any], command: ChartDataCommand
    ) -> Response:
        """
        Execute command as an async query.
        """
        # First, look for the chart query results in the cache.
        with contextlib.suppress(ChartDataCacheLoadError):
            logger.info(f"======result_format command.run return===={datetime.datetime.now()}")
            result = command.run(force_cached=True)
            if result is not None:
                logger.info(f"======result_format result is not None===={datetime.datetime.now()}")
                return self._send_chart_response(result)
        # Otherwise, kick off a background job to run the chart query.
        # Clients will either poll or be notified of query completion,
        # at which point they will call the /data/<cache_key> endpoint
        # to retrieve the results.
        async_command = CreateAsyncChartDataJobCommand()
        try:
            async_command.validate(request)
        except AsyncQueryTokenException:
            return self.response_401()

        logger.info(f"======result_format before async_command.run===={datetime.datetime.now()}")
        result = async_command.run(form_data, get_user_id())
        logger.info(f"======result_format after async_command.run===={datetime.datetime.now()}")
        return self.response(202, **result)

        def upload(self, data, headers):
            bucket_name = 'm-prod-superset-download'
        s3_file_name = headers.get("Content-Disposition")
        return self.generate_s3_response(data, headers, bucket_name, s3_file_name)


    def _send_chart_response(  # noqa: C901
        self,
        result: dict[Any, Any],
        form_data: dict[str, Any] | None = None,
        datasource: BaseDatasource | Query | None = None,
        oid: str = "",
    ) -> Response:
        result_type = result["query_context"].result_type
        result_format = result["query_context"].result_format

        logger.info(f"======result_format==={result_format}=={datetime.datetime.now()}")
        # Post-process the data so it matches the data presented in the chart.
        # This is needed for sending reports based on text charts that do the
        # post-processing of data, eg, the pivot table.
        if result_type == ChartDataResultType.POST_PROCESSED:
            result = apply_client_processing(result, form_data, datasource)

        if result_format in ChartDataResultFormat.table_like():
            # Verify user has permission to export file
            if not security_manager.can_access("can_csv", "Superset"):
                return self.response_403()

            if not result["queries"]:
                return self.response_400(_("Empty query result"))

            is_csv_format = result_format == ChartDataResultFormat.CSV

            if len(result["queries"]) == 1:
                # return single query results
                data = result["queries"][0]["data"]
                data_size = len(data.encode('utf-8')) if isinstance(data, str) else len(
                    data)
                if data_size > 10 * 1024 * 1024:  # 10 MB
                    if is_csv_format:
                        format = "csv"
                    else:
                        format = "xlsx"
                    #print("=======format1=======", format, str(datetime.datetime.now()))
                    logger.info(f"=======format1===={format}==={datetime.datetime.now()}")
                    if is_csv_format:
                        if isinstance(data, str):  # Convert string to bytes only if needed
                            file_data = io.BytesIO(data.encode('utf-8'))
                        else:
                            file_data = io.BytesIO(data)  # If already bytes, use as is
                    else:
                        if isinstance(data, str):
                            file_data = io.BytesIO(data.encode("utf-8"))  # Convert string to bytes
                        else:
                            file_data = io.BytesIO(data)  # Wrap bytes in BytesIO
                        #file_data = io.BytesIO(data)
                    logger.info(f"=======format2==={format}===={datetime.datetime.now()}")
                    headers = generate_download_headers(format)
                    logger.info(f"=======format3==={format}===={datetime.datetime.now()}")
                    return self.upload(file_data, headers)
                else:
                    if is_csv_format:
                        return CsvResponse(data, headers=generate_download_headers("csv"))
                    else:
                        return XlsxResponse(data, headers=generate_download_headers("xlsx"))

            # return multi-query results bundled as a zip file
            def _process_data(query_data: Any) -> Any:
                if result_format == ChartDataResultFormat.CSV:
                    encoding = app.config["CSV_EXPORT"].get("encoding", "utf-8")
                    return query_data.encode(encoding)
                return query_data

            files = {
                f"query_{idx + 1}.{result_format}": _process_data(query["data"])
                for idx, query in enumerate(result["queries"])
            }
            return Response(
                create_zip(files),
                headers=generate_download_headers("zip"),
                mimetype="application/zip",
            )

        if result_format == ChartDataResultFormat.JSON:
            queries = result["queries"]
            if security_manager.is_guest_user():
                for query in queries:
                    query.pop("query", None)
            with event_logger.log_context(f"{self.__class__.__name__}.json_dumps"):
                response_data = json.dumps(
                    {"result": queries},
                    default=json.json_int_dttm_ser,
                    ignore_nan=True,
                )
            resp = make_response(response_data, 200)
            resp.headers["Content-Type"] = "application/json; charset=utf-8"
            return resp

        return self.response_400(message=f"Unsupported result_format: {result_format}")

    @event_logger.log_this
    def _get_data_response(
        self,
        command: ChartDataCommand,
        force_cached: bool = False,
        form_data: dict[str, Any] | None = None,
        datasource: BaseDatasource | Query | None = None,
        oid: str = "",
    ) -> Response:
        try:
            result = command.run(force_cached=force_cached)
        except ChartDataCacheLoadError as exc:
            return self.response_422(message=exc.message)
        except ChartDataQueryFailedError as exc:
            return self.response_400(message=exc.message)

        return self._send_chart_response(result, form_data, datasource, oid)

    # pylint: disable=invalid-name
    def _load_query_context_form_from_cache(self, cache_key: str) -> dict[str, Any]:
        return QueryContextCacheLoader.load(cache_key)

    def _map_form_data_datasource_to_dataset_id(
        self, form_data: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "dashboard_id": form_data.get("form_data", {}).get("dashboardId"),
            "dataset_id": (
                form_data.get("datasource", {}).get("id")
                if isinstance(form_data.get("datasource"), dict)
                and form_data.get("datasource", {}).get("type")
                == DatasourceType.TABLE.value
                else None
            ),
            "slice_id": form_data.get("form_data", {}).get("slice_id"),
        }

    @logs_context(context_func=_map_form_data_datasource_to_dataset_id)
    def _create_query_context_from_form(
        self, form_data: dict[str, Any]
    ) -> QueryContext:
        """
        Create the query context from the form data.

        :param form_data: The chart form data
        :returns: The query context
        :raises ValidationError: If the request is incorrect
        """

        try:
            return ChartDataQueryContextSchema().load(form_data)
        except KeyError as ex:
            raise ValidationError("Request is incorrect") from ex
