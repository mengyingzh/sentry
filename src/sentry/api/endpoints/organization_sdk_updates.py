from __future__ import absolute_import
from datetime import datetime, timedelta
from sentry.api.bases.organization import OrganizationEndpoint

from itertools import groupby
from rest_framework.response import Response
from sentry.sdk_updates import SdkIndexState, SdkSetupState, get_suggested_updates

from sentry.utils.compat import map
from sentry.snuba import discover


def by_sdk_name(sdk):
    return sdk["sdk.name"]


def by_project_id(sdk):
    return sdk["project.id"]


class OrganizationSdkUpdates(OrganizationEndpoint):
    def get(self, request, organization):

        project_ids = self.get_requested_project_ids(request)
        projects = self.get_projects(request, organization, project_ids)

        result = discover.query(
            query="has:sdk.version",
            selected_columns=["project", "sdk.name", "sdk.version", "last_seen()"],
            orderby="-project",
            params={
                "start": datetime.now() - timedelta(days=1),
                "end": datetime.now(),
                "organization_id": organization.id,
                "project_id": [p.id for p in projects],
            },
            referrer="api.organization-sdk-updates",
        )

        # Build datastructure of the latest version of each SDK in use for each
        # project we have events for.
        sdks_by_project = [
            (
                project_id,
                [
                    {
                        "name": sdk_name,
                        "version": sorted(
                            sdks, key=lambda v: [int(u) for u in v["sdk.version"].split(".")]
                        ).pop()["sdk.version"],
                    }
                    for sdk_name, sdks in groupby(
                        sorted(sdks_used, key=by_sdk_name), key=by_sdk_name
                    )
                ],
            )
            for project_id, sdks_used in groupby(result["data"], key=by_project_id)
        ]
        sdks_by_project = dict(sdks_by_project)

        # Determine suggested upgrades for each project
        index_state = SdkIndexState()
        project_upgrade_suggestions = {
            project.id: map(
                lambda sdk: get_suggested_updates(
                    SdkSetupState(sdk["name"], sdk["version"], (), ()), index_state
                ),
                sdks_by_project.get(project.id, []),
            )
            for project in projects
        }

        return Response(project_upgrade_suggestions)
