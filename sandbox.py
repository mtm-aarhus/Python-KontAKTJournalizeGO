"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement, QueueStatus

from robot_framework.process import process
from robot_framework import config
from robot_framework import reset
import os
import json
from typing import Optional


def make_queue_element_with_payload(
    payload: dict | list,
    queue_name: str,
    reference: Optional[str] = None,
    created_by: Optional[str] = None,
    status: QueueStatus = QueueStatus.NEW, 
) -> QueueElement:
    # Validate & serialize
    data_str = json.dumps(payload, ensure_ascii=False)
    if len(data_str) > 2000:
        raise ValueError("data exceeds 2000 chars (column limit)")

    return QueueElement(
        queue_name=queue_name,
        status=status,
        data=data_str,
        reference=reference,
        created_by=created_by,
    )

# pylint: disable-next=unused-argum
orchestrator_connection = OrchestratorConnection(
    "KontAKTJournalizeGO",
    os.getenv("OpenOrchestratorSQL"),
    os.getenv("OpenOrchestratorKey"),
    None,
    None,
    None
)

client = reset.reset(orchestrator_connection)

USE_QUEUE = True

if USE_QUEUE:
    queue_element = None
    task_count = 0
    # Retry loop

    # Queue loop
    while task_count < config.MAX_TASK_COUNT:
        task_count += 1
        queue_element = orchestrator_connection.get_next_queue_element(config.QUEUE_NAME)

        if not queue_element:
            orchestrator_connection.log_info("Queue empty.")
            break  # Break queue loop

        process(orchestrator_connection, queue_element, client)
        orchestrator_connection.set_queue_element_status(queue_element.id, QueueStatus.DONE)



else:
    qe = make_queue_element_with_payload(
        payload={
            "mode": "create_case",
            "kontakt_case_id": 11,
            "title": "Aktindsigt i byggesag",
            "modtaget": "2026-06-25 00:00:00",
            "caseworker_email": "jadt@aarhus.dk",
        },
        queue_name="KontAKTJournalizeGO",
        reference="Sandbox",
        status=QueueStatus.NEW,
    )

    process(orchestrator_connection, qe, client)


