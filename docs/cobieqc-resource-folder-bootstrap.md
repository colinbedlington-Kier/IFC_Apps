# COBieQC resource folder bootstrap (Railway)

## Why Google Drive folder links are not ZIP downloads

`COBIEQC_XML_SOURCE_URL` is now treated as a **folder reference**. A Google Drive folder URL (for example `https://drive.google.com/drive/folders/...`) returns an HTML page, not a ZIP payload. Treating that URL like a binary download causes false archive errors.

## Why the old ZIP bootstrap failed

The previous XML/XSL bootstrap path assumed it could download and extract an archive. When the source URL was a Google Drive **folder** page, the response was HTML; archive checks/extraction then failed.

## Railway-safe resource supply (recommended)

Most reliable production setup:

1. Put the unzipped `xsl_xml` directory on a mounted volume or bake it into the image.
2. Point `COBIEQC_RESOURCE_DIR` at that directory (typically `/data/cobieqc/xsl_xml`).
3. Keep `COBIEQC_XML_SOURCE_URL` as an optional folder reference only.

If local resources are missing and automatic folder sync is unavailable, COBieQC is disabled gracefully and the rest of the app continues.

## Bootstrap precedence

1. Use `COBIEQC_RESOURCE_DIR` (or `/data/cobieqc/xsl_xml`) immediately if present and valid.
2. Else copy from packaged local fallback directories in the image.
3. Else attempt dedicated folder sync from `COBIEQC_XML_SOURCE_URL`.
4. Else disable COBieQC cleanly.

## Active vs deprecated variables

- Active:
  - `COBIEQC_RESOURCE_DIR`
  - `COBIEQC_XML_SOURCE_URL` (folder reference)
  - `COBIEQC_JAR_PATH`
  - `COBIEQC_JAR_SOURCE_URL`
- Deprecated for XML/XSL resources:
  - `COBIEQC_XML_ZIP_SOURCE_URL` (ignored)

## Expected directory structure

`COBIEQC_RESOURCE_DIR` should point to an `xsl_xml` folder that is non-empty and contains required XML/XSL files, for example:

```text
/data/cobieqc/xsl_xml/
  template.xml
  ...
  *.xsl
```

The bootstrap validates directory existence and expected `*.xml` + `*.xsl` content before enabling COBieQC.
