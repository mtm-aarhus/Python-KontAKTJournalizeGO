"""KontAKT → GetOrganized journalisation robot (KontAKTJournalizeGO).

GENOPLIVET 2026-11 fra sin git-historik (d75f323 i Python-KontAKTJournalize),
fordi migreringen fra GO til F2 tager længere tid end ventet. F2-robotten
(KontAKTJournalize) og denne kører side om side: en KontAKT-sag journaliseres i
den journal, den blev oprettet i (``cases.journal_system``), og KontAKT sender
køelementet til den robot, der hører til (app/journal.py). En administrator
vælger journalen for NYE sager under Administration > Indstillinger.

Queue-driven. Keeps a GO "AKT" journaliseringssag in sync with a KontAKT case and
journalises the correspondence + the delivery onto it. Mode is set by ``mode``
in the payload:

* ``create_case``      create the AKT case in GO with the case profile metadata
                       (Sagsprofil = MTM Aktindsigt, Facet = A53, Modtaget,
                       Title = Emne) and set the caseworker as CaseOwner. Calls
                       back with the new GO case number.
* ``update_metadata``  update Title / Modtaget / CaseOwner when they change in
                       KontAKT.
* ``journalize_email`` fetch a message from KontAKT, render it to PDF and add +
                       journalise it on the GO case - and every attachment with
                       it, each as its own document.
* ``sync_go``          bring the GO case in step with what KontAKT delivers, by
                       the plan KontAKT computes (upload / replace / delete /
                       keep + the aktliste).
* ``delete_doc``       delete a document from GO (by go_doc_id) after it was
                       deleted in KontAKT.

Modes the old robot had for queue elements created before ``sync_go``
(``journalize_ref``, ``journalize_folder``, ``generate_aktliste``) are gone: this
is a new queue, so no such element can exist.

The GO connection and the cached KontAKT credentials live on the ``Client``
opened in ``reset.open_all`` and are reused across queue elements.

OO config (same as the other KontAKT GO robots):
    Constant   GOApiURL          GO base URL (e.g. https://ad.go.aarhuskommune.dk)
    Credential GOAktApiUser      GO NTLM username + password
    Credential GOAdminUser       ONLY for deleting documents (see reset.Client)
    Credential KontAKTAPI        username = base URL, password = X-API-Key
"""
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
from urllib.parse import quote
import json
import os
import tempfile

import requests

from robot_framework import reset
from robot_framework.exceptions import CaseDeleted
from oomtm import go as oomtm_go
from oomtm import pdf as oomtm_pdf
from oomtm import sharepoint as sp  # sanitize_segment helper only

# The AKT cases live under the /aktindsigt web (the caseworker's proven create
# endpoint is …/aktindsigt/_goapi/Cases). Metadata/upload/close route by CaseId,
# so they work from the root web.
GO_AKT_WEB = "/aktindsigt"

# Fixed case-profile metadata for an MTM aktindsigt journaliseringssag (the
# GUID-paired term-store values the caseworker supplied).
SAGSPROFIL_AKT = "165;#MTM Aktindsigt"
SAGSPROFIL_AKT_TERM = "MTM Aktindsigt|b0976f16-ee0b-4d5f-8755-1048d4b796ba"
SAGSPROFIL_AKT_FIELD = "pc53b1eb189a451bbe8688ffaa073059"
FACET = "4;#A53 Aktindsigtsanmodning mv."
FACET_TERM = "A53 Aktindsigtsanmodning mv.|db5714c1-9346-47e6-b7a7-2230bf997699"
FACET_FIELD = "hd725939cd4d495483312d36ba720a4d"


# ----- Deleted in KontAKT ----------------------------------------------------


def _check_gone(resp) -> None:
    """Stop cleanly if what this queue element is about was deleted in KontAKT.

    KontAKT answers HTTP 410 with ``{"deleted": "case"|"reference"|"document"}``
    when the caseworker deleted the KontAKT case, the sag/mappe or the document
    while this element waited in the queue. Not an error and not retryable, so
    the queue framework marks the element done and takes the next one.
    """
    if resp is None or resp.status_code != 410:
        return
    try:
        body = resp.json() or {}
    except ValueError:
        body = {}
    if body.get("deleted"):
        raise CaseDeleted(body.get("note") or f"{body['deleted']} deleted in KontAKT")


def process(
    orchestrator_connection: OrchestratorConnection,
    queue_element: QueueElement | None = None,
    client: "reset.Client | None" = None,
) -> None:
    orchestrator_connection.log_trace("Running process.")
    if queue_element is None:
        raise RuntimeError("KontAKTJournalizeGO is queue-driven; no queue_element given.")
    if client is None:  # manual run outside the queue framework
        client = reset.open_all(orchestrator_connection)

    payload = json.loads(queue_element.data or "{}")
    mode = (payload.get("mode") or "").strip()
    case_id = int(payload["kontakt_case_id"])

    if mode == "create_case":
        _create_case(orchestrator_connection, client, case_id, payload)
    elif mode == "update_metadata":
        _update_metadata(orchestrator_connection, client, case_id, payload)
    elif mode == "journalize_email":
        _journalize_email(orchestrator_connection, client, case_id, payload)
    elif mode == "sync_go":
        _sync_go(orchestrator_connection, client, case_id, payload)
    elif mode == "delete_doc":
        _delete_doc(orchestrator_connection, client, case_id, payload)
    else:
        raise RuntimeError(f"Unknown KontAKTJournalizeGO mode: {mode!r}")


# ----- create_case -----------------------------------------------------------


def _create_case(oc, client, case_id, payload):
    path = f"/api/v1/cases/{case_id}/go-journal/created"
    title = str(payload.get("title") or "").strip() or f"Aktindsigt (KontAKT-sag {case_id})"
    modtaget = str(payload.get("modtaget") or "").strip()
    caseworker_email = str(payload.get("caseworker_email") or "").strip()
    oc.log_info(f"GO create_case for KontAKT case {case_id}: {title!r}")
    try:
        metadata_xml = _create_metadata_xml(title, modtaget)
        created = oomtm_go.create_case(
            client.go_session, base_url=client.go_url,
            metadata_xml=metadata_xml, case_type_prefix="AKT", web=GO_AKT_WEB,
        )
        go_case_no = created.get("CaseID")
        relative_url = created.get("CaseRelativeUrl")
        oc.log_info(f"GO case created: {go_case_no} ({relative_url})")
    except CaseDeleted:
        raise
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"GO create_case failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
        raise
    if not go_case_no:
        _callback(oc, client, path, {"ok": False, "note": "GO returnerede intet sagsnummer."})
        return

    owner_note = _try_set_owner(oc, client, go_case_no, caseworker_email, relative_url)
    _callback(oc, client, path, {"ok": True, "go_case_no": go_case_no,
                                 "case_relative_url": relative_url, "note": owner_note})
    oc.log_info(f"GO create_case done: {go_case_no}")


# ----- update_metadata -------------------------------------------------------


def _update_metadata(oc, client, case_id, payload):
    path = f"/api/v1/cases/{case_id}/go-journal/updated"
    go_case_no = str(payload.get("go_case_no") or "").strip()
    if not go_case_no:
        _callback(oc, client, path, {"ok": False, "note": "Sagen mangler et GO-sagsnummer."})
        return
    title = payload.get("title")
    modtaget = payload.get("modtaget")
    caseworker_email = str(payload.get("caseworker_email") or "").strip()
    oc.log_info(f"GO update_metadata for {go_case_no} (case {case_id})")
    try:
        xml = _update_metadata_xml(title, modtaget)
        if xml:
            oomtm_go.set_case_metadata(client.go_session, base_url=client.go_url,
                                       case_id=go_case_no, metadata_xml=xml)
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"GO update_metadata failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
        raise
    owner_note = _try_set_owner(oc, client, go_case_no, caseworker_email) if caseworker_email else None
    _callback(oc, client, path, {"ok": True, "note": owner_note})
    oc.log_info(f"GO update_metadata done: {go_case_no}")


# ----- journalize_email ------------------------------------------------------


def _journalize_email(oc, client, case_id, payload):
    """Én besked bliver ét PDF-dokument i GO - og hver vedhæftning sit eget.

    GO tager kun filer, så mailen renderes til PDF (LibreOffice, html -> pdf).
    Billeder i teksten kommer med, fordi KontAKT allerede har lagt dem ind som
    data:-URI'er (api.email_content).

    VEDHÆFTNINGERNE KOMMER MED. Den gamle GO-robot lagde kun selve mailen i GO,
    så et foto eller en PDF, ansøgeren sendte, fandtes kun i KontAKT. Nu lægges
    hver fil op ved siden af mailen, med samme dato og korrespondance.

    DEN HER FUNKTION SKAL KUNNE KØRE IGEN. Den kaldes igen, hver gang en
    indgående mails filer lander i KontAKT (de kommer EFTER selve mailen), og
    køelementer prøves igen ved fejl. Derfor:

      * mailen lægges kun op, hvis KontAKT ikke allerede kender dens GO-id, og
        id'et meldes STRAKS, før vedhæftningerne;
      * kun vedhæftninger uden GO-id lægges op;
      * filnavnene er faste (mailens og vedhæftningens KontAKT-id står i dem),
        så en kørsel, der døde efter GO tog imod filen, men før den nåede at
        melde det, overskriver sin egen fil i stedet for at lave en dublet.
    """
    email_id = int(payload["email_id"])
    go_case_no = str(payload.get("go_case_no") or "").strip()
    path = f"/api/v1/cases/{case_id}/emails/{email_id}/journalized"
    oc.log_info(f"GO journalize_email email={email_id} -> {go_case_no}")
    if not go_case_no:
        _callback(oc, client, path, {"ok": False, "note": "Sagen er endnu ikke oprettet i GO."})
        return
    try:
        email = _kontakt_get(client, f"/api/v1/cases/{case_id}/emails/{email_id}")
    except CaseDeleted:
        raise
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"GO journalize_email: kunne ikke hente beskeden: {exc!r}")
        _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
        raise

    udgaaende = (email.get("direction") or "") == "outbound"
    # Den rigtige dato for hver retning: en indgående mail HAR ingen sent_at.
    tidspunkt = email.get("sent_at") if udgaaende else email.get("received_at")
    korrespondance = "Udgående" if udgaaende else "Indgående"

    doc_id = str(email.get("go_document_id") or "").strip()
    if not doc_id:
        try:
            pdf_bytes, file_name = _email_to_pdf(oc, email, tidspunkt, udgaaende)
            meta = _doc_metadata_xml(
                title=(email.get("subject") or f"E-mail {email_id}"),
                date=_date_only(tidspunkt), korrespondance=korrespondance,
            )
            nyt = oomtm_go.upload_document(
                client.go_session, base_url=client.go_url, case_id=go_case_no,
                file_bytes=pdf_bytes, file_name=file_name, metadata_xml=meta,
            )
            doc_id = str(nyt or "").strip()
        except Exception as exc:  # pylint: disable=broad-except
            oc.log_info(f"GO journalize_email failed: {exc!r}")
            _callback(oc, client, path, {"ok": False, "note": str(exc)[:400]})
            raise
        if not doc_id:
            _callback(oc, client, path, {"ok": False, "note": "GO returnerede intet dokument-id."})
            raise RuntimeError(f"GO returned no DocId for email {email_id}.")
        # Meldes før markeringen og før vedhæftningerne: dør kørslen bagefter,
        # ved KontAKT i det mindste, hvilket dokument mailen er.
        _callback(oc, client, path, {"ok": True, "doc_id": doc_id})
        _mark(oc, client, [doc_id])

    meldt = _vedhaeftninger(oc, client, case_id, email_id, go_case_no, email,
                            tidspunkt, korrespondance)
    if meldt:
        _callback(oc, client, path, {"ok": True, "doc_id": doc_id, "attachments": meldt})
        _mark(oc, client, [m["go_doc_id"] for m in meldt])
    oc.log_info(f"GO journalize_email done email={email_id} doc={doc_id}, "
                f"{len(meldt)} vedhæftning(er)")


def _vedhaeftninger(oc, client, case_id, email_id, go_case_no, email,
                    tidspunkt, korrespondance) -> list:
    """Læg mailens filer op ved siden af den. Returnerer [{id, go_doc_id}]."""
    meldt = []
    for a in (email.get("attachments") or []):
        if str(a.get("go_document_id") or "").strip():
            continue
        aid = int(a["id"])
        if not a.get("has_file"):
            # Indgående mails filer kommer én ad gangen EFTER mailen. KontAKT
            # beder om en ny kørsel, når filen er landet.
            oc.log_info(f"GO journalize_email: vedhæftning {aid} er ikke hentet "
                        f"endnu - kommer med, når den lander.")
            continue
        navn = (a.get("file_name") or f"vedhaeftning-{aid}").strip()
        try:
            data = _kontakt_bytes(
                client, f"/api/v1/cases/{case_id}/emails/{email_id}/attachments/{aid}/file")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 410:
                oc.log_info(f"GO journalize_email: vedhæftning {aid} har ingen fil "
                            f"i KontAKT endnu.")
                continue
            raise
        meta = _doc_metadata_xml(title=os.path.splitext(navn)[0] or navn,
                                 date=_date_only(tidspunkt), korrespondance=korrespondance)
        go_doc_id = oomtm_go.upload_document(
            client.go_session, base_url=client.go_url, case_id=go_case_no,
            file_bytes=data,
            # Fast navn pr. vedhæftning - se _journalize_email.
            file_name=_safe_name(f"E-mail {email_id} - bilag {aid} - {navn}")[:160],
            metadata_xml=meta,
        )
        if go_doc_id:
            meldt.append({"id": aid, "go_doc_id": str(go_doc_id)})
    return meldt


def _mark(oc, client, doc_ids) -> None:
    """Markér som sagsakt. Best-effort: dokumentet ER i GO, og en manglende
    markering må ikke få kørslen til at lægge det op igen."""
    ids = [d for d in doc_ids if str(d or "").strip()]
    if not ids:
        return
    try:
        oomtm_go.mark_as_case_record(client.go_session, base_url=client.go_url, doc_ids=ids)
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"GO: kunne ikke markere {ids} som sagsakt: {exc!r}")


# ----- journalise documents (sync_go) -----------------------------------------


def _fetch_content(client, case_id, doc_id, local_path) -> bool:
    """Stream a document's stored bytes from KontAKT's file store to ``local_path``.
    Returns False if the file isn't in the store (404)."""
    r = requests.get(
        f"{client.kontakt_base}/api/v1/cases/{case_id}/documents/{doc_id}/content",
        headers={"X-API-Key": client.kontakt_key}, timeout=300, stream=True,
    )
    _check_gone(r)
    if r.status_code == 404:
        return False
    r.raise_for_status()
    with open(local_path, "wb") as fh:
        for chunk in r.iter_content(1 << 20):
            if chunk:
                fh.write(chunk)
    return True


def _upload_delivery_file(client, case_id, go_case_no, doc_id, name, folder_path="",
                          created_folders=None, title=None):
    """Fetch one delivered document's bytes from KontAKT's file store and upload it
    to the GO case under ``folder_path`` (a sub-folder in the GO Dokumenter
    library). Returns the GO DocId, or None if the file isn't in the store.

    ``name`` is the filename the applicant's copy carries, so the file in GO can
    be matched to its row in the aktliste. ``title`` is the caseworker's own title
    for the document; without one the filename stands in."""
    name = (name or f"dokument-{doc_id}")
    meta = _doc_metadata_xml(title=(title or os.path.splitext(name)[0]),
                             korrespondance="Udgående")
    # The upload streams from the temp file rather than reading it into memory, so a
    # 2 GB video costs one chunk of RSS instead of two copies of itself. That means
    # the whole thing has to happen INSIDE the TemporaryDirectory, not after it.
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, _safe_name(name))
        if not _fetch_content(client, case_id, doc_id, local):
            return None
        return oomtm_go.upload_document(
            client.go_session, base_url=client.go_url, case_id=go_case_no,
            file_path=local, file_name=name, metadata_xml=meta, folder_path=folder_path,
            created_folders=created_folders,
        )


def _sync_go(oc, client, case_id, payload):
    """Bring the GO case in step with what KontAKT actually delivers.

    Fired the first time a delivery link is created, after every change to a
    sag that has already been delivered, and once more when the case is closed.
    KontAKT hands over a **plan** - what to upload, replace, delete and leave
    alone, plus whether the aktliste needs redoing - so this robot never has to
    work out the difference itself, and a retry or a missed change can't leave
    GO permanently wrong: the next run recomputes the plan from current data. A
    run with nothing to do is normal.

    Per document failures are reported and then raised, so the element is retried:
    the callback has already recorded what did succeed, so a retry only redoes
    what is left.
    """
    path = f"/api/v1/cases/{case_id}/go-journal/documents-journalized"
    src = str(payload.get("source_case_id") or "").strip()
    query = f"?source_case_id={quote(src)}" if src else ""
    try:
        plan = _kontakt_get(client, f"/api/v1/cases/{case_id}/go-journal/plan{query}")
    except CaseDeleted:
        raise
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"GO sync: kunne ikke hente planen: {exc!r}")
        _callback(oc, client, path, {"ok": False, "source_case_id": src or None,
                                     "note": f"Kunne ikke hente planen: {exc}"[:400]})
        raise
    go_case_no = str(plan.get("go_case_no") or payload.get("go_case_no") or "").strip()
    if not go_case_no:
        _callback(oc, client, path, {"ok": False, "source_case_id": src or None,
                                     "note": "Sagen er endnu ikke oprettet i GO."})
        return

    sager = plan.get("sager") or []
    if not sager:
        oc.log_info("GO sync: ingen sager at holde i takt.")
        return

    results, failed_total = [], 0
    for sag in sager:
        result = _sync_one_sag(oc, client, case_id, go_case_no, sag)
        failed_total += len(result["failed"])
        results.append(result)
    _callback(oc, client, path, {"ok": True, "sager": results})
    if failed_total:
        raise RuntimeError(f"{failed_total} dokument(er) kunne ikke journaliseres i GO.")


def _sync_one_sag(oc, client, case_id, go_case_no, sag) -> dict:
    """Execute one sag's part of the plan. Every failure lands in ``failed`` so
    the rest of the sag still gets done - except CaseDeleted: a case deleted in
    KontAKT ends the element, there is nothing left to do it for."""
    src = str(sag.get("source_case_id") or "").strip()
    folder = sp.sanitize_segment(src)[:80].strip() or "ukendt-sag"
    created_folders: set = set()
    out = {"source_case_id": src, "uploaded": [], "replaced": [],
           "deleted": [], "kept": sag.get("keep") or [], "failed": []}
    oc.log_info(
        f"GO sync {src}: {len(sag.get('upload') or [])} ny, "
        f"{len(sag.get('replace') or [])} erstat, {len(sag.get('delete') or [])} slet.")

    # Gone from the delivery -> gone from GO. Deleting means un-marking the case
    # record first, which only GOAdminUser may do (see Client.go_delete_session).
    for item in (sag.get("delete") or []):
        try:
            _go_delete(client, item.get("go_doc_id"))
            out["deleted"].append(item.get("doc_id"))
        except Exception as exc:  # pylint: disable=broad-except
            oc.log_info(f"GO sync: kunne ikke slette DocId {item.get('go_doc_id')}: {exc!r}")
            out["failed"].append({"doc_id": item.get("doc_id"), "note": str(exc)[:300]})

    # Changed since it was filed: remove the old copy, then file the new one. Not
    # an overwrite - a document marked as a case record can't be overwritten by
    # the ordinary account, and a half-overwritten file is worse than a replaced one.
    for item in (sag.get("replace") or []):
        try:
            _go_delete(client, item.get("go_doc_id"))
        except Exception as exc:  # pylint: disable=broad-except
            oc.log_info(f"GO sync: kunne ikke fjerne den gamle kopi af "
                        f"doc={item.get('doc_id')}: {exc!r}")
            out["failed"].append({"doc_id": item.get("doc_id"), "note": str(exc)[:300]})
            continue
        _file_one(oc, client, case_id, go_case_no, item, folder, created_folders,
                  out, "replaced")

    for item in (sag.get("upload") or []):
        _file_one(oc, client, case_id, go_case_no, item, folder, created_folders,
                  out, "uploaded")

    _mark(oc, client, [m["go_doc_id"] for m in out["uploaded"] + out["replaced"]
                       if m.get("go_doc_id")])

    # The dokumentliste changed with the documents, so the copy in GO has to be
    # the new one. The previous pair is removed first, for the same reason as above.
    if sag.get("aktliste"):
        try:
            for old in (sag.get("aktliste_go_doc_ids") or []):
                try:
                    _go_delete(client, old)
                except Exception as exc:  # pylint: disable=broad-except
                    oc.log_info(f"GO sync: gammel aktliste {old} kunne ikke fjernes: {exc!r}")
            new_ids = _file_aktliste(oc, client, case_id, go_case_no, src)
            if new_ids:
                out["aktliste_go_doc_ids"] = new_ids
        except CaseDeleted:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            oc.log_info(f"GO sync: aktlisten for {src} fejlede: {exc!r}")
            out["failed"].append({"doc_id": None, "note": f"Aktliste: {exc}"[:300]})
    oc.log_info(f"GO sync {src} færdig: {len(out['uploaded'])} lagt op, "
                f"{len(out['replaced'])} erstattet, {len(out['deleted'])} slettet, "
                f"{len(out['failed'])} fejlede.")
    return out


def _file_one(oc, client, case_id, go_case_no, item, folder, created_folders, out, key):
    try:
        go_doc_id = _upload_delivery_file(client, case_id, go_case_no, item["doc_id"],
                                          item.get("file_name"), folder, created_folders,
                                          title=item.get("title"))
        if go_doc_id:
            out[key].append({"doc_id": item["doc_id"], "go_doc_id": str(go_doc_id),
                             "token": item.get("token")})
        else:
            # No file in KontAKT's store: not this robot's problem to fix, but it
            # must not silently look like success either.
            out["failed"].append({"doc_id": item["doc_id"],
                                  "note": "Dokumentet har ingen fil i filstoret."})
    except CaseDeleted:
        raise
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"GO sync: doc={item.get('doc_id')} kunne ikke lægges op: {exc!r}")
        out["failed"].append({"doc_id": item.get("doc_id"), "note": str(exc)[:300]})


def _go_delete(client, go_doc_id) -> None:
    """Delete one document from GO by DocId, as GOAdminUser."""
    raw = str(go_doc_id or "").strip()
    if not raw:
        return
    oomtm_go.delete_document(client.go_delete_session(), base_url=client.go_url,
                             doc_id=int(raw))


def _delete_doc(oc, client, case_id, payload):
    """Delete a document from GO after it was deleted in KontAKT. Best-effort;
    no callback (the KontAKT row is already gone).

    Runs as GOAdminUser, the only account allowed to un-mark a document as a case
    record - which is what deleting a journalised document requires. Every other
    GO operation in this robot stays on GOAktApiUser: see Client.go_delete_session.
    """
    raw = str(payload.get("go_doc_id") or "").strip()
    oc.log_info(f"GO delete_doc case={case_id} go_doc_id={raw}")
    if not raw:
        return
    # GO documents DocId as an Int. KontAKT stores it in a text column, so it
    # arrives as a string - send it as the number it is.
    try:
        go_doc_id = int(raw)
    except ValueError:
        oc.log_info(f"GO delete_doc: '{raw}' er ikke et DocId - springer over.")
        return
    oomtm_go.delete_document(client.go_delete_session(), base_url=client.go_url,
                             doc_id=go_doc_id)
    oc.log_info(f"GO delete_doc done: {go_doc_id}")


def _file_aktliste(oc, client, case_id, go_case_no, source_case_id) -> list[str]:
    """Render the aktliste (PDF + Excel) in KontAKT and file both onto the GO case.

    Returns the new DocIds, so the next regeneration can remove exactly these two
    instead of relying on an overwrite. Tells KontAKT which content the filed copy
    reflects, so the caseworker's "aktlisten er aktuel" reading stays true."""
    path = f"/api/v1/cases/{case_id}/aktliste/generated"
    oc.log_info(f"Aktliste case={case_id} sag={source_case_id} -> {go_case_no}")
    new_ids: list[str] = []
    try:
        data = _kontakt_get(client, f"/api/v1/cases/{case_id}/aktliste?source_case_id={quote(source_case_id)}")
        rows = data.get("rows") or []
        sagsnummer = data.get("sagsnummer") or source_case_id
        content_token = data.get("content_token")
        if not rows:
            oc.log_info("Aktliste: ingen dokumenter - springer over.")
            return new_ids

        # KontAKT renders the aktliste; this robot only files it. One renderer for
        # the whole system means the applicant's copy, the caseworker's preview and
        # the copy in GO cannot drift apart.
        xlsx_bytes = _kontakt_bytes(
            client, f"/api/v1/cases/{case_id}/aktliste.xlsx?source_case_id={quote(source_case_id)}")
        pdf_bytes = _kontakt_bytes(
            client, f"/api/v1/cases/{case_id}/aktliste.pdf?source_case_id={quote(source_case_id)}")

        # Stable filenames: the same two documents each time, so GO holds one
        # aktliste per sag rather than a pile of dated ones.
        files = [(f"Aktliste - {sagsnummer}.xlsx", xlsx_bytes),
                 (f"Aktliste - {sagsnummer}.pdf", pdf_bytes)]

        undermappe = sp.sanitize_segment(source_case_id)[:80].strip() or "ukendt-sag"
        created_folders: set = set()
        for name, blob in files:
            meta = _doc_metadata_xml(title=os.path.splitext(name)[0], korrespondance="Internt")
            go_doc_id = oomtm_go.upload_document(
                client.go_session, base_url=client.go_url, case_id=go_case_no,
                file_bytes=blob, file_name=name, metadata_xml=meta,
                folder_path=undermappe, created_folders=created_folders,
            )
            if go_doc_id:
                new_ids.append(str(go_doc_id))
        _mark(oc, client, new_ids)
    except CaseDeleted:
        raise
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"Aktliste failed: {exc!r}")
        _callback(oc, client, path, {"ok": False, "source_case_id": source_case_id, "note": str(exc)[:400]})
        raise
    # Tell KontAKT which content the aktliste now reflects, so it counts as current.
    _callback(oc, client, path, {"ok": True, "source_case_id": source_case_id,
                                 "content_token": content_token,
                                 "go_doc_ids": new_ids})
    oc.log_info(f"Aktliste opdateret for {sagsnummer}: {len(rows)} rækker, 2 filer.")
    return new_ids


# ----- metadata XML builders -------------------------------------------------


def _xml_attr(value) -> str:
    """Escape a value for use inside a double-quoted XML attribute."""
    return (str(value or "")
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace('"', "&quot;"))


def _create_metadata_xml(title: str, modtaget: str) -> str:
    parts = [
        f'ows_Title="{_xml_attr(title)}"',
        'ows_CaseStatus="Åben"',
        f'ows_Sagsprofil_AKT="{_xml_attr(SAGSPROFIL_AKT)}"',
        f'ows_{SAGSPROFIL_AKT_FIELD}="{_xml_attr(SAGSPROFIL_AKT_TERM)}"',
        f'ows_Facet="{_xml_attr(FACET)}"',
        f'ows_{FACET_FIELD}="{_xml_attr(FACET_TERM)}"',
    ]
    if modtaget:
        parts.append(f'ows_Modtaget="{_xml_attr(modtaget)}"')
    return '<z:row xmlns:z="#RowsetSchema" ' + " ".join(parts) + " />"


def _update_metadata_xml(title, modtaget) -> str | None:
    parts = []
    if title:
        parts.append(f'ows_Title="{_xml_attr(title)}"')
    if modtaget:
        parts.append(f'ows_Modtaget="{_xml_attr(modtaget)}"')
    if not parts:
        return None
    return '<z:row xmlns:z="#RowsetSchema" ' + " ".join(parts) + " />"


def _doc_metadata_xml(*, title: str, date: str = "", korrespondance: str = "") -> str:
    parts = [f'ows_Title="{_xml_attr(title)}"']
    if date:
        parts.append(f'ows_Dato="{_xml_attr(date)}"')
    if korrespondance:
        parts.append(f'ows_Korrespondance="{_xml_attr(korrespondance)}"')
    parts.append('ows_CCMMustBeOnPostList="0"')
    return '<z:row xmlns:z="#RowsetSchema" ' + " ".join(parts) + " />"


# ----- e-mail rendering ------------------------------------------------------


def _email_to_pdf(oc, email: dict, tidspunkt, udgaaende: bool):
    """Render a KontAKT e-mail to PDF bytes via LibreOffice (html -> pdf).
    Returns (pdf_bytes, file_name). The file name carries the KontAKT id, so a
    re-run overwrites its own file instead of adding a second one."""
    soffice = oomtm_pdf.ensure_libreoffice(log=oc.log_info)
    subject = email.get("subject") or "E-mail"
    header = (
        f"<p style='color:#555;font-size:12px'>"
        f"<b>Fra:</b> {_xml_attr(email.get('from_address') or '')}<br>"
        f"<b>Til:</b> {_xml_attr(email.get('to_addresses') or '')}<br>"
        f"<b>{'Sendt' if udgaaende else 'Modtaget'}:</b> {_xml_attr(tidspunkt or '')}<br>"
        f"<b>Emne:</b> {_xml_attr(subject)}</p><hr>"
    )
    body = email.get("body_html") or (
        "<pre>" + _xml_attr(email.get("body_text") or "") + "</pre>"
    )
    html = (f"<!doctype html><html><head><meta charset='utf-8'></head>"
            f"<body>{header}{body}</body></html>")
    with tempfile.TemporaryDirectory() as tmp:
        html_path = os.path.join(tmp, "email.html")
        with open(html_path, "w", encoding="utf-8") as fh:
            fh.write(html)
        pdf_path = oomtm_pdf.office_to_pdf(html_path, tmp, soffice_path=soffice)
        if pdf_path is None:
            raise RuntimeError("LibreOffice kunne ikke konvertere e-mailen til PDF.")
        with open(pdf_path, "rb") as fh:
            pdf_bytes = fh.read()
    file_name = _safe_name(
        f"E-mail {email.get('id')} - {_date_only(tidspunkt)} - {subject}")[:120] + ".pdf"
    return pdf_bytes, file_name


# ----- caseowner (best-effort) -----------------------------------------------


def _try_set_owner(oc, client, go_case_no, caseworker_email, case_relative_url=None) -> str | None:
    """Set CaseOwner from the caseworker e-mail. Best-effort: never fails the
    job. ``case_relative_url`` is the create response's CaseRelativeUrl (on a
    fresh create); on a later update it's resolved from ows_CaseUrl. Returns a
    short note when it couldn't be set, else None."""
    if not caseworker_email:
        return None
    try:
        ok = oomtm_go.set_case_owner(
            client.go_session, base_url=client.go_url, case_id=go_case_no,
            caseworker_email=caseworker_email, case_relative_url=case_relative_url,
        )
        if not ok:
            return f"Sagsbehandleren {caseworker_email} blev ikke fundet i GO."
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"set_case_owner failed (non-fatal): {exc!r}")
        return f"Sagsansvarlig kunne ikke sættes automatisk ({str(exc)[:120]})."
    return None


# ----- helpers ---------------------------------------------------------------


def _date_only(value) -> str:
    """'2026-06-25T10:58:00' / '2026-06-25 10:58:00' -> '25-06-2026' for GO."""
    s = str(value or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return f"{s[8:10]}-{s[5:7]}-{s[0:4]}"
    return s[:10]


def _safe_name(name: str) -> str:
    keep = []
    for ch in str(name or ""):
        keep.append(ch if ch not in '\\/:*?"<>|' else " ")
    return " ".join("".join(keep).split()) or "dokument"


def _kontakt_get(client, path: str) -> dict:
    r = requests.get(
        f"{client.kontakt_base}{path}",
        headers={"X-API-Key": client.kontakt_key, "Accept": "application/json"},
        timeout=60,
    )
    _check_gone(r)
    r.raise_for_status()
    return r.json()


def _kontakt_bytes(client, path: str) -> bytes:
    """A file from KontAKT: a mail attachment or the rendered aktliste.

    A plain 410 (no ``deleted`` body) passes ``_check_gone`` and comes out of
    ``raise_for_status`` as an HTTPError - that is an attachment whose bytes
    have not landed yet, and the caller tells the two apart.
    """
    r = requests.get(
        f"{client.kontakt_base}{path}",
        headers={"X-API-Key": client.kontakt_key},
        timeout=300,
    )
    _check_gone(r)
    r.raise_for_status()
    if not r.content:
        raise RuntimeError(f"KontAKT returned an empty file for {path}")
    return r.content


def _callback(oc, client, path: str, body: dict) -> None:
    try:
        resp = requests.post(
            f"{client.kontakt_base}{path}",
            headers={"X-API-Key": client.kontakt_key, "Content-Type": "application/json"},
            json=body, timeout=30,
        )
    except Exception as exc:  # pylint: disable=broad-except
        oc.log_info(f"Callback to KontAKT failed: {exc!r}")
        return
    # Outside the except: a network blip stays harmless, but "deleted in KontAKT"
    # must reach the framework instead of being swallowed as a broad Exception.
    _check_gone(resp)
