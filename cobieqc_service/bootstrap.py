import logging
import shutil
from pathlib import Path

LOGGER = logging.getLogger("ifc_app.cobieqc.bootstrap")

DATA_ROOT = Path("/data/cobieqc")
DATA_JAR = DATA_ROOT / "CobieQcReporter.jar"
DATA_RESOURCES = DATA_ROOT / "xsl_xml"

JAR_SOURCE_CANDIDATES = [
    Path("/vendor/cobieqc/CobieQcReporter.jar"),
    Path("/app/vendor/cobieqc/CobieQcReporter.jar"),
    Path("/app/CobieQcReporter/CobieQcReporter.jar"),
    Path("/app/COBieQC/CobieQcReporter/CobieQcReporter.jar"),
    Path("/opt/COBieQC/CobieQcReporter/CobieQcReporter.jar"),
]

RESOURCE_SOURCE_CANDIDATES = [
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

    if DATA_JAR.exists():
        LOGGER.info("COBieQC bootstrap: existing JAR kept at %s", DATA_JAR)
    else:
        jar_source = _first_existing_file(JAR_SOURCE_CANDIDATES)
        if jar_source:
            shutil.copy2(jar_source, DATA_JAR)
            LOGGER.info("COBieQC bootstrap: copied JAR from %s to %s", jar_source, DATA_JAR)
        else:
            LOGGER.warning(
                "COBieQC bootstrap: JAR missing at %s and no source found in %s",
                DATA_JAR,
                [str(p) for p in JAR_SOURCE_CANDIDATES],
            )

    if DATA_RESOURCES.exists():
        LOGGER.info("COBieQC bootstrap: existing resource dir kept at %s", DATA_RESOURCES)
    else:
        resource_source = _first_existing_dir(RESOURCE_SOURCE_CANDIDATES)
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
                [str(p) for p in RESOURCE_SOURCE_CANDIDATES],
            )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    bootstrap_cobieqc_assets()


if __name__ == "__main__":
    main()
