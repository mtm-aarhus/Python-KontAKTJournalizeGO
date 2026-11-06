"""This module handles resetting the state of the computer so the robot can work with a clean slate.

For this robot the "state" is the GO connection and the cached OO credentials.
``open_all`` opens them and returns a :class:`Client`; ``reset`` re-opens them,
so the queue framework can reconnect on a retry instead of reconnecting for
every single document.
"""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from oomtm import go as oomtm_go


class Client:
    """Live GO connection + cached KontAKT credentials.

    Opened once per run by ``open_all`` and reused across every queue element,
    so a 2000-document case doesn't re-authenticate to GO 2000 times.
    """

    def __init__(self, orchestrator_connection: OrchestratorConnection):
        go_cred = orchestrator_connection.get_credential("GOAktApiUser")
        self.go_url = orchestrator_connection.get_constant("GOApiURL").value
        self.go_user = go_cred.username
        self.go_pass = go_cred.password
        self.go_session = oomtm_go.session(go_cred.username, go_cred.password)
        kontakt = orchestrator_connection.get_credential("KontAKTAPI")
        self.kontakt_base = kontakt.username
        self.kontakt_key = kontakt.password
        # Deleting a journalised document means un-marking it as a case record,
        # which GOAktApiUser is not allowed to do ("Bruger har ikke rettigheder
        # til at afmarkere dokumet"). So deletion — and ONLY deletion — runs as
        # GOAdminUser. Opened on first use rather than up front: a run that never
        # deletes anything never authenticates with the privileged account, and it
        # cannot be reached by accident from the other operations.
        self._oc = orchestrator_connection
        self._go_admin_session = None

    def go_delete_session(self):
        """The privileged session, for document deletion in GO and nothing else."""
        if self._go_admin_session is None:
            cred = self._oc.get_credential("GOAdminUser")
            self._oc.log_info("Opening GO admin connection (deletion only).")
            self._go_admin_session = oomtm_go.session(cred.username, cred.password)
        return self._go_admin_session


def reset(orchestrator_connection: OrchestratorConnection) -> Client:
    """Clean up, close/kill all programs, then (re)open the connections.

    Returns the freshly-opened :class:`Client` so the queue framework can reuse
    it across queue elements (and reconnect by calling ``reset`` again)."""
    orchestrator_connection.log_trace("Resetting.")
    clean_up(orchestrator_connection)
    close_all(orchestrator_connection)
    kill_all(orchestrator_connection)
    return open_all(orchestrator_connection)


def clean_up(orchestrator_connection: OrchestratorConnection) -> None:
    """Do any cleanup needed to leave a blank slate."""
    orchestrator_connection.log_trace("Doing cleanup.")


def close_all(orchestrator_connection: OrchestratorConnection) -> None:
    """Gracefully close all applications used by the robot."""
    orchestrator_connection.log_trace("Closing all applications.")


def kill_all(orchestrator_connection: OrchestratorConnection) -> None:
    """Forcefully close all applications used by the robot."""
    orchestrator_connection.log_trace("Killing all applications.")


def open_all(orchestrator_connection: OrchestratorConnection) -> Client:
    """Open all connections used by the robot and return them as a :class:`Client`."""
    orchestrator_connection.log_trace("Opening GO connection.")
    return Client(orchestrator_connection)
