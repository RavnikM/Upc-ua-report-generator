"""
OS Printing Service using `win32print` for Windows industrial deployments,
with a safe cross-platform simulation fallback on Linux/macOS.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import win32api  # type: ignore[import-not-found]
    import win32print  # type: ignore[import-not-found]

    HAS_WIN32PRINT = True
except ImportError:
    HAS_WIN32PRINT = False


@dataclass(frozen=True)
class PrintJobResult:
    """Outcome of a PDF print dispatch operation."""

    success: bool
    printer_name: str
    simulated: bool
    message: str


def print_pdf_document(
    pdf_path: str | Path,
    printer_name: Optional[str] = None,
) -> PrintJobResult:
    """
    Send a generated PDF report to the configured Windows OS printer via `win32print`.

    On non-Windows platforms (or when `pywin32` is not installed), logs the print
    dispatch and returns a simulated success result for local development/testing.
    """
    resolved_path = Path(pdf_path).resolve()
    if not resolved_path.is_file():
        msg = f"PDF file does not exist for printing: {resolved_path}"
        logger.error(msg)
        return PrintJobResult(
            success=False,
            printer_name=printer_name or "DEFAULT",
            simulated=not HAS_WIN32PRINT,
            message=msg,
        )

    if sys.platform == "win32" and HAS_WIN32PRINT:
        try:
            target_printer = printer_name or win32print.GetDefaultPrinter()
            # Verify printer exists by opening a handle briefly
            handle = win32print.OpenPrinter(target_printer)
            win32print.ClosePrinter(handle)

            # Dispatch PDF to Windows print spooler
            win32api.ShellExecute(
                0,
                "printto" if printer_name else "print",
                str(resolved_path),
                f'"{target_printer}"' if printer_name else None,
                ".",
                0,
            )
            msg = f"Dispatched '{resolved_path.name}' to Windows printer '{target_printer}'."
            logger.info(msg)
            return PrintJobResult(
                success=True,
                printer_name=target_printer,
                simulated=False,
                message=msg,
            )
        except Exception as exc:
            msg = f"win32print error while printing '{resolved_path.name}': {exc}"
            logger.exception(msg)
            return PrintJobResult(
                success=False,
                printer_name=printer_name or "DEFAULT",
                simulated=False,
                message=msg,
            )

    # Non-Windows / development environment fallback
    target_printer = printer_name or "Simulated_Industrial_Printer_01"
    msg = (
        f"[Simulated win32print on {sys.platform}] Queued PDF '{resolved_path.name}' "
        f"({resolved_path.stat().st_size} bytes) to printer '{target_printer}'."
    )
    logger.info(msg)
    return PrintJobResult(
        success=True,
        printer_name=target_printer,
        simulated=True,
        message=msg,
    )
