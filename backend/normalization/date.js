export function toIsoStringOrNull(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    const fromEpochMillis = new Date(value);
    if (!Number.isNaN(fromEpochMillis.getTime())) {
      return fromEpochMillis.toISOString();
    }
    return null;
  }

  const parsed = new Date(String(value));
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString();
}

export function resolveArchivePublishedAt(archiveRecord) {
  const fromDateHeader = toIsoStringOrNull(archiveRecord?.dateHeader);
  if (fromDateHeader) {
    return fromDateHeader;
  }

  const internalDateValue = Number.parseInt(String(archiveRecord?.internalDateMs ?? ""), 10);
  const fromInternalDate = toIsoStringOrNull(internalDateValue);
  if (fromInternalDate) {
    return fromInternalDate;
  }

  const fromIngestedAt = toIsoStringOrNull(archiveRecord?.ingestedAt);
  if (fromIngestedAt) {
    return fromIngestedAt;
  }

  return new Date().toISOString();
}

export function toUtcDayBucket(value) {
  const iso = toIsoStringOrNull(value);
  if (!iso) {
    return "unknown-day";
  }
  return iso.slice(0, 10);
}
