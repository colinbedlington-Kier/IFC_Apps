import logging
import os
import shutil
from pathlib import Path

LOGGER = logging.getLogger("ifc_app.cobieqc.bootstrap")

DATA_ROOT = Path("/data/cobieqc")
DATA_JAR = DATA_ROOT / "CobieQcReporter.jar"
DATA_RESOURCES = DATA_ROOT / "xsl_xml"

_CWD = Path.cwd()
_APP_ROOT = Path(__file__).resolve().parents[1]

JAR_SOURCE_CANDIDATES = [
    _CWD / "vendor" / "cobieqc" / "CobieQcReporter.jar",
    _CWD / "CobieQcReporter" / "CobieQcReporter.jar",
    _CWD / "COBieQC" / "CobieQcReporter" / "CobieQcReporter.jar",
    _APP_ROOT / "vendor" / "cobieqc" / "CobieQcReporter.jar",
    _APP_ROOT / "CobieQcReporter" / "CobieQcReporter.jar",
    _APP_ROOT / "COBieQC" / "CobieQcReporter" / "CobieQcReporter.jar",
    Path("/vendor/cobieqc/CobieQcReporter.jar"),
    Path("/app/vendor/cobieqc/CobieQcReporter.jar"),
    Path("/app/CobieQcReporter/CobieQcReporter.jar"),
    Path("/app/COBieQC/CobieQcReporter/CobieQcReporter.jar"),
    Path("/opt/COBieQC/CobieQcReporter/CobieQcReporter.jar"),
]

RESOURCE_SOURCE_CANDIDATES = [
    _CWD / "vendor" / "cobieqc" / "xsl_xml",
    _CWD / "CobieQcReporter" / "xsl_xml",
    _CWD / "COBieQC" / "CobieQcReporter" / "xsl_xml",
    _APP_ROOT / "vendor" / "cobieqc" / "xsl_xml",
    _APP_ROOT / "CobieQcReporter" / "xsl_xml",
    _APP_ROOT / "COBieQC" / "CobieQcReporter" / "xsl_xml",
    Path("/vendor/cobieqc/xsl_xml"),
    Path("/app/vendor/cobieqc/xsl_xml"),
    Path("/app/CobieQcReporter/xsl_xml"),
    Path("/app/COBieQC/CobieQcReporter/xsl_xml"),
    Path("/opt/COBieQC/CobieQcReporter/xsl_xml"),
]


def _first_existing_file(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _first_existing_dir(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def bootstrap_cobieqc_assets() -> None:
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    LOGGER.info("COBieQC bootstrap: data root ready at %s", DATA_ROOT)

    configured_jar_source = os.getenv("COBIEQC_JAR_SOURCE", "").strip()
    configured_resource_source = os.getenv("COBIEQC_RESOURCE_SOURCE", "").strip()
    jar_candidates = list(JAR_SOURCE_CANDIDATES)
    resource_candidates = list(RESOURCE_SOURCE_CANDIDATES)
    if configured_jar_source:
        jar_candidates.insert(0, Path(configured_jar_source).expanduser())
    if configured_resource_source:
        resource_candidates.insert(0, Path(configured_resource_source).expanduser())

    if DATA_JAR.exists():
        LOGGER.info("COBieQC bootstrap: existing JAR kept at %s", DATA_JAR)
    else:
        jar_source = _first_existing_file(jar_candidates)
        if jar_source:
            shutil.copy2(jar_source, DATA_JAR)
            LOGGER.info("COBieQC bootstrap: copied JAR from %s to %s", jar_source, DATA_JAR)
        else:
            LOGGER.warning(
                "COBieQC bootstrap: JAR missing at %s and no source found in %s",
                DATA_JAR,
                [str(p) for p in jar_candidates],
            )

    if DATA_RESOURCES.exists():
        LOGGER.info("COBieQC bootstrap: existing resource dir kept at %s", DATA_RESOURCES)
    else:
        resource_source = _first_existing_dir(resource_candidates)
        if resource_source:
            shutil.copytree(resource_source, DATA_RESOURCES)
            LOGGER.info(
                "COBieQC bootstrap: copied resources from %s to %s",
                resource_source,
                DATA_RESOURCES,
            )
        else:
            LOGGER.warning(
                "COBieQC bootstrap: resource dir missing at %s and no source found in %s",
                DATA_RESOURCES,
                [str(p) for p in resource_candidates],
            )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    bootstrap_cobieqc_assets()


if __name__ == "__main__":
    main()
