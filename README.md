# Python-KontAKTJournalizeGO

Keeps a **GetOrganized (GO)** journaliseringssag (an `AKT` case) in sync with a
**KontAKT** aktindsigt case, and journalises the correspondence and the delivery
onto it.

Revived in November 2026 from the history of `Python-KontAKTJournalize`
(commit `d75f323`), because the move from GO to F2 is taking longer than
planned. Both robots run side by side: each KontAKT case is journalised in the
journal it was created in (`cases.journal_system`), and KontAKT sends the queue
element to the matching robot (`app/journal.py`). An administrator chooses the
journal for **new** cases under *Administration > Indstillinger*.

Queue: `KontAKTJournalizeGO`.

## What it does

Queue-driven; the mode is set by `mode` in the input:

- **`create_case`** creates the `AKT` case in GO with the MTM aktindsigt case
  profile (Sagsprofil = *MTM Aktindsigt*, Facet = *A53 Aktindsigtsanmodning mv.*,
  Modtaget, Title = Emne) and sets the KontAKT caseworker as **CaseOwner**
  (best-effort). Calls back with the new GO case number.
- **`update_metadata`** updates Title / Modtaget and CaseOwner when they change
  in KontAKT.
- **`journalize_email`** fetches a message from KontAKT, renders it to PDF
  (LibreOffice) and adds + journalises it on the GO case, **together with every
  attachment**, each as its own document. Safe to re-run: already-filed files
  are skipped, and file names are fixed per KontAKT id so a re-run overwrites
  rather than duplicates.
- **`sync_go`** fetches KontAKT's plan (`…/go-journal/plan`) and carries it out:
  upload new delivered documents, replace changed ones, delete removed ones,
  and refile the aktliste (PDF + Excel, rendered by KontAKT). Reports back what
  it did. Fired when a delivery link is created, after every change to a
  delivered sag, and once more when the case is closed.
- **`delete_doc`** deletes a document from GO (by `go_doc_id`) after it was
  deleted in KontAKT.

Delivered documents go into a sub-folder per source case (named by
`{source_case_id}`) under the GO case's `Dokumenter` library.

## Input

| Field | Modes | Meaning |
|-------|-------|---------|
| `mode` | all | see above |
| `kontakt_case_id` | all | KontAKT case id (callback target) |
| `go_case_no` | most | the GO `AKT-…` case number |
| `title` | create/update | Emne → `ows_Title` |
| `modtaget` | create/update | `YYYY-MM-DD HH:MM:SS` → `ows_Modtaget` |
| `caseworker_email` | create/update | the caseworker to set as CaseOwner |
| `email_id` | journalize_email | KontAKT `case_emails` row to journalise |
| `source_case_id` | sync_go | limit the sync to one source case |
| `go_doc_id` | delete_doc | the GO DocId to delete |

## Output / callbacks

- create → `POST /api/v1/cases/{id}/go-journal/created` `{ok, go_case_no, case_relative_url, note}`
- update → `POST /api/v1/cases/{id}/go-journal/updated` `{ok, note}`
- email → `POST /api/v1/cases/{id}/emails/{email_id}/journalized` `{ok, doc_id, attachments:[{id, go_doc_id}]}`
- sync → `POST /api/v1/cases/{id}/go-journal/documents-journalized` `{ok, sager:[…]}`
- aktliste → `POST /api/v1/cases/{id}/aktliste/generated` `{ok, source_case_id, content_token, go_doc_ids}`
- delete_doc → no callback (best-effort; the KontAKT row is already gone)

(or `{ok: false, note}` on failure)

## Required configuration

- Constant `GOApiURL`: GO base URL (e.g. `https://ad.go.aarhuskommune.dk`)
- Credential `GOAktApiUser`: GO NTLM username + password
- Credential `GOAdminUser`: used **only** to delete documents (un-marking a case
  record needs it). Opened on first use, so a run that deletes nothing never
  logs in with it. See `reset.Client.go_delete_session`.
- Credential `KontAKTAPI`: username = base URL, password = API key

## Dependencies

The shared [`oomtm`](https://github.com/mtm-aarhus/oomtm) library (`go`, `pdf`),
pinned to a commit. E-mail/HTML → PDF uses LibreOffice headless, installed
without admin rights on first use by `oomtm.pdf.ensure_libreoffice`.
