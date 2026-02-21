const STORAGE_KEYS = {
  apiBase: "brokerage_mvp_api_base",
  authToken: "brokerage_mvp_auth_token",
  priority: "brokerage_mvp_priority_companies",
  ignored: "brokerage_mvp_ignored_companies",
  recipients: "brokerage_mvp_digest_recipients",
  schedule: "brokerage_mvp_digest_schedule",
  dictionary: "brokerage_mvp_company_dictionary"
};

const defaultApiBase = "https://pkeday-brokerage-insights-api.onrender.com";
const notificationsApiBaseFallback = defaultApiBase;
const legacyApiBases = new Set([
  "https://pkeday-ai-webapp-brokerage-api.onrender.com",
  "https://pkeday-ai-webapp-api.onrender.com"
]);
const fallbackAiCategories = [
  "earnings_results_boardmeeting",
  "results_intimation_date",
  "earning_call_registration",
  "earning_call_audio_recording",
  "earning_call_transcript",
  "analyst_meeting",
  "participating_in_conference",
  "investor_day",
  "agm_announcement",
  "board_meeting",
  "board_meeting_intimation",
  "press_release",
  "auditors_report",
  "annual_report",
  "earning_presentation",
  "ma",
  "divestment",
  "contract_awarded",
  "fund_raising",
  "change_in_management",
  "credit_rating",
  "trading_stopping",
  "investor_conference",
  "not_eligible",
  "newspaper_announcement",
  "agm_outcome",
  "postal_ballot",
  "esops",
  "insider_trading",
  "substantial_transaction",
  "share_pledge",
  "change_in_auditor",
  "business_update",
  "surveillance_reply",
  "record_date_intimation",
  "share_transfer_relodgement_report",
  "certificate_under_regulation_74_5",
  "non_applicability_regulation_27_2",
  "others"
];

const extractionEndpointCatalog = {
  trigger: [
    "/api/extraction/runs",
    "/api/extraction/run",
    "/api/extractions/run",
    "/api/extractions/runs",
    "/api/jobs/extraction",
    "/api/reports/extraction/run"
  ],
  runs: [
    "/api/extraction/runs",
    "/api/extractions/runs",
    "/api/reports/extraction/runs",
    "/api/report-extraction/runs"
  ],
  reports: [
    "/api/extracted-reports",
    "/api/extraction/reports",
    "/api/extractions/reports",
    "/api/reports/extracted",
    "/api/reports/extraction/items"
  ]
};

const defaultDictionary = [
  {
    canonical: "Reliance Industries",
    ticker: "NSE:RIL",
    aliases: ["Reliance", "Reliance Inds", "Reliance Industries Ltd", "RIL"]
  },
  {
    canonical: "UltraTech Cement",
    ticker: "NSE:ULTRACEMCO",
    aliases: ["UltraTech", "Ultra Tech", "UTCEM", "UltraTech Cement Ltd"]
  },
  {
    canonical: "ICICI Bank",
    ticker: "NSE:ICICIBANK",
    aliases: ["ICICI", "ICICI BK", "ICICI Bank Ltd"]
  },
  {
    canonical: "Adani Ports",
    ticker: "NSE:ADANIPORTS",
    aliases: ["APSEZ", "Adani Port", "Adani Ports SEZ"]
  },
  {
    canonical: "Vodafone Idea",
    ticker: "NSE:IDEA",
    aliases: ["Voda Idea", "Vodafone", "VI"]
  }
];

const seedReports = [
  {
    id: "ax-ril-results-1902",
    broker: "Axis Capital",
    company: "Reliance Industries Ltd",
    type: "Results Update",
    coverage: "Large Cap",
    time: "2026-02-19T08:42:00+05:30",
    summary:
      "EBITDA beat is quality-led from refining mix, not pure volume uplift. FY27E EPS +3.1 percent, TP moved from 3,210 to 3,360 with Neutral retained due to rerating limits.",
    sentiment: "neutral",
    links: {
      archive: "#",
      pdf: "#",
      gmail: "#"
    }
  },
  {
    id: "ax-ultra-update-1902",
    broker: "Axis Capital",
    company: "Ultra Tech",
    type: "General Update",
    coverage: "Cement",
    time: "2026-02-19T11:25:00+05:30",
    summary:
      "Cost reset thesis is pulled forward as fuel spread narrows. Margin bridge implies FY27E EBITDA per ton +6.4 percent while capex cadence remains unchanged.",
    sentiment: "bullish",
    links: {
      archive: "#",
      pdf: "#",
      gmail: "#"
    }
  },
  {
    id: "ax-roundup-ril-repeat-1902",
    broker: "Axis Capital",
    company: "Reliance",
    type: "General Update",
    coverage: "Roundup",
    time: "2026-02-19T11:25:00+05:30",
    summary: "Latest releases section repeats prior Reliance result takeaways from morning note.",
    sentiment: "neutral",
    duplicateOf: "ax-ril-results-1902",
    links: {
      archive: "#",
      pdf: "#",
      gmail: "#"
    }
  },
  {
    id: "kotak-ril-init-1902",
    broker: "Kotak Institutional Equities",
    company: "RIL",
    type: "Initiation",
    coverage: "Large Cap",
    time: "2026-02-19T07:58:00+05:30",
    summary:
      "Initiates with Add on medium-term cash conversion from O2C plus new-energy option value. Bear-case explicitly stresses a weaker chemicals downcycle.",
    sentiment: "bullish",
    links: {
      archive: "#",
      pdf: "#",
      gmail: "#"
    }
  },
  {
    id: "kotak-icici-update-1902",
    broker: "Kotak Institutional Equities",
    company: "ICICI BK",
    type: "General Update",
    coverage: "BFSI",
    time: "2026-02-19T09:31:00+05:30",
    summary:
      "Liability-side repricing remains the core watch item. Estimate block is unchanged, but the note warns against assuming CASA normalization pace too early.",
    sentiment: "neutral",
    links: {
      archive: "#",
      pdf: "#",
      gmail: "#"
    }
  },
  {
    id: "jeff-ultra-results-1902",
    broker: "Jefferies India",
    company: "UltraTech",
    type: "Results Update",
    coverage: "Cement",
    time: "2026-02-19T10:14:00+05:30",
    summary:
      "Pricing realization surprise offsets freight inflation. Jefferies flags south-region volume lag as the only material variance versus broad buy-side narrative.",
    sentiment: "bullish",
    links: {
      archive: "#",
      pdf: "#",
      gmail: "#"
    }
  }
];

const state = {
  section: "brokerage",
  view: "dashboard",
  apiBase: loadApiBase(),
  auth: {
    token: loadString(STORAGE_KEYS.authToken, ""),
    user: null,
    gmailConnected: false,
    ingestionPreferences: null,
    loading: false
  },
  ingestSetup: {
    open: false,
    loading: false,
    saving: false,
    labels: [],
    selectedLabelIds: [],
    query: "",
    maxResults: 30,
    scheduleEnabled: true,
    scheduleTime: "07:30",
    scheduleTimezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Kolkata",
    dateFrom: "",
    dateTo: "",
    startFromNow: true,
    resetCursor: false,
    statusMessage: ""
  },
  archives: {
    items: [],
    brokerFilter: "All",
    search: "",
    fetchedAt: null,
    total: 0,
    page: 1,
    pageSize: 20
  },
  notifications: {
    items: [],
    total: 0,
    limit: 50,
    page: 1,
    totalPages: 1,
    exchange: "NSE",
    symbol: "",
    aiLabelFilter: "all",
    source: "",
    loading: false,
    error: "",
    lastSyncAt: null,
    lastSyncStats: null,
    aiCategories: [],
    reviewSaving: false,
    reviewSavingKey: "",
    reviewFeedback: "",
    suggestionSaving: false,
    suggestionFeedback: "",
    suggestions: [],
    suggestionsTotal: 0,
    suggestionsLoaded: false,
    suggestionsLoading: false,
    suggestionsError: "",
    suggestionContext: null
  },
  extraction: {
    endpointStatus: {
      trigger: "unknown",
      runs: "unknown",
      reports: "unknown"
    },
    endpoints: {
      trigger: "",
      runs: "",
      reports: ""
    },
    statusMessage: "Checking extraction endpoints...",
    runStarting: false,
    abortingRunId: "",
    reprocessExisting: false,
    runsLoading: false,
    reportsLoading: false,
    runsError: "",
    reportsError: "",
    runs: [],
    runsTotal: 0,
    lastRun: null,
    reports: [],
    reportsTotal: 0,
    fetchedAt: null,
    filters: {
      broker: "All",
      company: "",
      reportType: "All",
      includeDuplicates: false
    }
  },
  priorityCompanies: loadList(STORAGE_KEYS.priority, ["Reliance Industries", "UltraTech Cement", "ICICI Bank"]),
  ignoredCompanies: loadList(STORAGE_KEYS.ignored, ["Adani Ports", "Vodafone Idea"]),
  digestRecipients: loadList(STORAGE_KEYS.recipients, ["pkeday@gmail.com"]),
  digestSchedule: loadString(STORAGE_KEYS.schedule, "Daily 07:30 IST"),
  dictionary: loadDictionary(),
  filters: {
    broker: "All",
    type: "All",
    search: "",
    includeDuplicates: false
  },
  companyView: {
    selected: "Reliance Industries",
    sort: "latest"
  },
  digestApi: {
    loading: false,
    error: "",
    lastSentAt: null,
    preview: null
  },
  pipelineMessage: ""
};

const refs = {
  backendStatus: document.getElementById("backend-status"),
  authStatus: document.getElementById("auth-status"),
  pipelineStatus: document.getElementById("pipeline-status"),
  sectionButtons: Array.from(document.querySelectorAll(".section-tab")),
  brokerageViewTabs: document.getElementById("brokerage-view-tabs"),
  tabButtons: Array.from(document.querySelectorAll(".tab")),
  views: {
    dashboard: document.getElementById("view-dashboard"),
    archive: document.getElementById("view-archive"),
    company: document.getElementById("view-company"),
    notifications: document.getElementById("view-notifications"),
    settings: document.getElementById("view-settings"),
    digest: document.getElementById("view-digest")
  },
  googleConnectBtn: document.getElementById("google-connect-btn"),
  signoutBtn: document.getElementById("signout-btn"),
  ingestSetupBtn: document.getElementById("ingest-setup-btn"),
  runIngestBtn: document.getElementById("run-ingest-btn"),
  sendDigestBtn: document.getElementById("send-digest-btn"),
  ingestSetupModal: document.getElementById("ingest-setup-modal"),
  ingestSetupCloseBtn: document.getElementById("ingest-setup-close-btn"),
  ingestSetupCancelBtn: document.getElementById("ingest-setup-cancel-btn"),
  ingestSetupSaveBtn: document.getElementById("ingest-setup-save-btn"),
  ingestSetupResetDataBtn: document.getElementById("ingest-setup-reset-data-btn"),
  ingestSetupRefreshLabelsBtn: document.getElementById("ingest-setup-refresh-labels-btn"),
  ingestSetupSelectAllBtn: document.getElementById("ingest-setup-select-all-btn"),
  ingestSetupClearAllBtn: document.getElementById("ingest-setup-clear-all-btn"),
  ingestSetupQuery: document.getElementById("ingest-setup-query"),
  ingestSetupMaxResults: document.getElementById("ingest-setup-max-results"),
  ingestSetupDateFrom: document.getElementById("ingest-setup-date-from"),
  ingestSetupDateTo: document.getElementById("ingest-setup-date-to"),
  ingestSetupTime: document.getElementById("ingest-setup-time"),
  ingestSetupTimezone: document.getElementById("ingest-setup-timezone"),
  ingestSetupStartNow: document.getElementById("ingest-setup-start-now"),
  ingestSetupResetCursor: document.getElementById("ingest-setup-reset-cursor"),
  ingestSetupLabels: document.getElementById("ingest-setup-labels"),
  ingestSetupStatus: document.getElementById("ingest-setup-status"),
  filterBroker: document.getElementById("filter-broker"),
  filterType: document.getElementById("filter-type"),
  filterSearch: document.getElementById("filter-search"),
  filterDuplicates: document.getElementById("filter-duplicates"),
  chipRow: document.getElementById("control-chip-row"),
  kpiGrid: document.getElementById("kpi-grid"),
  brokerLanes: document.getElementById("broker-lanes"),
  runExtractionBtn: document.getElementById("run-extraction-btn"),
  refreshExtractionBtn: document.getElementById("refresh-extraction-btn"),
  extractionReprocessExisting: document.getElementById("extraction-reprocess-existing"),
  extractionStatusNote: document.getElementById("extraction-status-note"),
  extractionLastRun: document.getElementById("extraction-last-run"),
  extractionRunsTable: document.getElementById("extraction-runs-table"),
  extractedFilterBroker: document.getElementById("extracted-filter-broker"),
  extractedFilterCompany: document.getElementById("extracted-filter-company"),
  extractedFilterType: document.getElementById("extracted-filter-type"),
  extractedFilterIncludeDuplicates: document.getElementById("extracted-filter-include-duplicates"),
  refreshExtractedBtn: document.getElementById("refresh-extracted-btn"),
  extractedReportsMeta: document.getElementById("extracted-reports-meta"),
  extractedReportsTable: document.getElementById("extracted-reports-table"),
  archiveBrokerFilter: document.getElementById("archive-broker-filter"),
  archiveSearch: document.getElementById("archive-search"),
  archiveRefreshBtn: document.getElementById("archive-refresh-btn"),
  archiveSummary: document.getElementById("archive-summary"),
  archiveTable: document.getElementById("archive-table"),
  archivePrevBtn: document.getElementById("archive-prev-btn"),
  archiveNextBtn: document.getElementById("archive-next-btn"),
  archivePageLabel: document.getElementById("archive-page-label"),
  notificationsExchangeSelect: document.getElementById("notifications-exchange-select"),
  notificationsSymbolInput: document.getElementById("notifications-symbol-input"),
  notificationsAiLabelFilter: document.getElementById("notifications-ai-label-filter"),
  notificationsLimitSelect: document.getElementById("notifications-limit-select"),
  notificationsRefreshBtn: document.getElementById("notifications-refresh-btn"),
  notificationsReviewFeedback: document.getElementById("notifications-review-feedback"),
  notificationsSuggestionPanel: document.getElementById("notifications-suggestion-panel"),
  notificationsSuggestionMeta: document.getElementById("notifications-suggestion-meta"),
  notificationsSuggestionCategory: document.getElementById("notifications-suggestion-category"),
  notificationsSuggestionComment: document.getElementById("notifications-suggestion-comment"),
  notificationsSuggestionSubmitBtn: document.getElementById("notifications-suggestion-submit-btn"),
  notificationsSuggestionClearContextBtn: document.getElementById("notifications-suggestion-clear-context-btn"),
  notificationsSuggestionFeedback: document.getElementById("notifications-suggestion-feedback"),
  notificationsSuggestionList: document.getElementById("notifications-suggestion-list"),
  notificationsMeta: document.getElementById("notifications-meta"),
  notificationsTable: document.getElementById("notifications-table"),
  notificationsPrevBtn: document.getElementById("notifications-prev-btn"),
  notificationsNextBtn: document.getElementById("notifications-next-btn"),
  notificationsPageLabel: document.getElementById("notifications-page-label"),
  companySelect: document.getElementById("company-select"),
  companySort: document.getElementById("company-sort"),
  companyTimeline: document.getElementById("company-timeline"),
  companyWorkbench: document.getElementById("company-workbench"),
  dictionaryTable: document.getElementById("dictionary-table"),
  priorityList: document.getElementById("priority-list"),
  ignoreList: document.getElementById("ignore-list"),
  recipientList: document.getElementById("recipient-list"),
  scheduleInput: document.getElementById("schedule-input"),
  digestPreview: document.getElementById("digest-preview"),
  dictionaryForm: document.getElementById("dictionary-form"),
  dictionaryCanonical: document.getElementById("dictionary-canonical"),
  dictionaryTicker: document.getElementById("dictionary-ticker"),
  dictionaryAliases: document.getElementById("dictionary-aliases"),
  priorityForm: document.getElementById("priority-form"),
  priorityInput: document.getElementById("priority-input"),
  ignoreForm: document.getElementById("ignore-form"),
  ignoreInput: document.getElementById("ignore-input"),
  recipientForm: document.getElementById("recipient-form"),
  recipientInput: document.getElementById("recipient-input"),
  scheduleSaveBtn: document.getElementById("schedule-save-btn")
};

let notificationsRequestSeq = 0;
let notificationsAbortController = null;

init();

async function init() {
  handleAuthCallbackFromHash();
  hydrateBrokerFilter();
  hydrateCompanySelect();
  bindEvents();
  renderAll();
  await Promise.allSettled([
    checkBackendStatus(),
    refreshAuthState(),
    fetchNotificationCategories(),
    fetchNotifications(),
    refreshExtractionWorkspace()
  ]);
}

function bindEvents() {
  const scheduleArchiveSearch = debounce(() => {
    state.archives.page = 1;
    renderArchiveView();
  }, 140);
  const scheduleNotificationsSearch = debounce(() => {
    state.notifications.page = 1;
    void fetchNotifications();
  }, 220);
  const scheduleExtractionCompanySearch = debounce(() => {
    state.extraction.filters.company = (refs.extractedFilterCompany?.value ?? "").trim();
    renderExtractionWorkspace();
  }, 180);

  for (const button of refs.sectionButtons) {
    button.addEventListener("click", () => {
      const section = button.dataset.section;
      if (!section) {
        return;
      }

      const normalizedSection = section === "notifications" ? "notifications" : "brokerage";
      if (normalizedSection === state.section) {
        return;
      }

      state.section = normalizedSection;
      renderViewState();

      if (state.section === "notifications" && state.notifications.items.length === 0 && !state.notifications.loading) {
        void fetchNotifications();
      }
    });
  }

  for (const button of refs.tabButtons) {
    button.addEventListener("click", () => {
      const view = button.dataset.view;
      if (!view) {
        return;
      }
      state.section = "brokerage";
      state.view = view;
      renderViewState();
      if (view === "digest") {
        void fetchDigestPreviewFromApi();
      }
    });
  }

  refs.googleConnectBtn.addEventListener("click", async () => {
    await startGoogleAuth();
  });

  refs.signoutBtn.addEventListener("click", async () => {
    await signOut();
  });

  refs.ingestSetupBtn.addEventListener("click", async () => {
    await openIngestSetupModal();
  });

  const closeIngestSetup = () => {
    closeIngestSetupModal();
  };

  refs.ingestSetupCloseBtn.addEventListener("click", closeIngestSetup);
  refs.ingestSetupCancelBtn.addEventListener("click", closeIngestSetup);
  refs.ingestSetupModal.addEventListener("click", (event) => {
    if (event.target instanceof HTMLElement && event.target.hasAttribute("data-ingest-close")) {
      closeIngestSetup();
    }
  });
  refs.ingestSetupRefreshLabelsBtn.addEventListener("click", async () => {
    await refreshIngestSetupLabelsOnly();
  });
  refs.ingestSetupSelectAllBtn.addEventListener("click", () => {
    state.ingestSetup.selectedLabelIds = state.ingestSetup.labels.map((label) => label.id);
    renderIngestSetupModal();
  });
  refs.ingestSetupClearAllBtn.addEventListener("click", () => {
    state.ingestSetup.selectedLabelIds = [];
    renderIngestSetupModal();
  });
  refs.ingestSetupSaveBtn.addEventListener("click", async () => {
    await saveIngestSetup();
  });
  if (refs.ingestSetupResetDataBtn) {
    refs.ingestSetupResetDataBtn.addEventListener("click", async () => {
      await resetPipelineDataForCurrentUser();
    });
  }
  refs.ingestSetupLabels.addEventListener("change", (event) => {
    const input = event.target;
    if (!(input instanceof HTMLInputElement) || input.type !== "checkbox") {
      return;
    }

    const labelId = String(input.dataset.labelId ?? "").trim();
    if (!labelId) {
      return;
    }

    if (input.checked) {
      if (!state.ingestSetup.selectedLabelIds.includes(labelId)) {
        state.ingestSetup.selectedLabelIds.push(labelId);
      }
    } else {
      state.ingestSetup.selectedLabelIds = state.ingestSetup.selectedLabelIds.filter((value) => value !== labelId);
    }
  });

  refs.ingestSetupStartNow.addEventListener("change", () => {
    if (refs.ingestSetupStartNow.checked && refs.ingestSetupResetCursor.checked) {
      refs.ingestSetupResetCursor.checked = false;
    }
    state.ingestSetup.startFromNow = refs.ingestSetupStartNow.checked;
    state.ingestSetup.resetCursor = refs.ingestSetupResetCursor.checked;
  });

  refs.ingestSetupResetCursor.addEventListener("change", () => {
    if (refs.ingestSetupResetCursor.checked && refs.ingestSetupStartNow.checked) {
      refs.ingestSetupStartNow.checked = false;
    }
    state.ingestSetup.startFromNow = refs.ingestSetupStartNow.checked;
    state.ingestSetup.resetCursor = refs.ingestSetupResetCursor.checked;
  });

  refs.runIngestBtn.addEventListener("click", async () => {
    if (!state.auth.token) {
      setPipelineMessage("Sign in with Google before running ingest.");
      return;
    }

    refs.runIngestBtn.disabled = true;
    setPipelineMessage("Running Gmail ingest...");

    try {
      const response = await apiFetch("/api/gmail/ingest", {
        method: "POST",
        body: JSON.stringify({
          includeAttachments: true
        })
      });

      const summary = response.summary;
      const extraction = response.extraction || null;
      const fetchedCount = Number(summary.fetchedMessages || 0);
      const archivedCount = Number(summary.archivedCount || 0);
      const skippedCount = Number(summary.skippedCount || 0);
      const cursorAfter = Number(summary.cursorAfterEpoch || 0);
      const cursorLabel = cursorAfter > 0 ? new Date(cursorAfter * 1000).toLocaleString() : "not set";
      if (fetchedCount === 0) {
        setPipelineMessage(
          `Ingest complete: fetched 0 messages. Cursor is ${cursorLabel}. If you expected older emails, open Ingest setup and check "Backfill older emails (reset cursor to oldest)".`
        );
      } else {
        const extractionNote =
          extraction?.run?.id && extraction?.queued
            ? ` Extraction queued (${extraction.run.id}).`
            : "";
        setPipelineMessage(
          `Ingest complete: fetched ${fetchedCount}, archived ${archivedCount}, skipped ${skippedCount}.${extractionNote}`
        );
      }
      await fetchArchives();
      if (extraction?.run?.id) {
        await refreshExtractionWorkspace();
      }
      if (state.view === "digest") {
        await fetchDigestPreviewFromApi();
      }
      renderAllDataViews();
    } catch (error) {
      setPipelineMessage(`Ingest failed: ${error.message}`);
    } finally {
      refs.runIngestBtn.disabled = false;
    }
  });

  refs.sendDigestBtn.addEventListener("click", () => {
    void (async () => {
      state.section = "brokerage";
      state.view = "digest";
      renderViewState();
      await sendDigestPreviewNow();
    })();
  });

  refs.filterBroker.addEventListener("change", (event) => {
    state.filters.broker = event.target.value;
    renderDashboard();
  });

  refs.filterType.addEventListener("change", (event) => {
    state.filters.type = event.target.value;
    renderDashboard();
  });

  refs.filterSearch.addEventListener("input", (event) => {
    state.filters.search = event.target.value.trim();
    renderDashboard();
  });

  refs.filterDuplicates.addEventListener("change", (event) => {
    state.filters.includeDuplicates = event.target.checked;
    renderDashboard();
  });

  if (refs.runExtractionBtn) {
    refs.runExtractionBtn.addEventListener("click", async () => {
      await triggerExtractionRun();
    });
  }

  if (refs.refreshExtractionBtn) {
    refs.refreshExtractionBtn.addEventListener("click", async () => {
      await refreshExtractionWorkspace();
    });
  }

  if (refs.extractionReprocessExisting) {
    refs.extractionReprocessExisting.addEventListener("change", (event) => {
      state.extraction.reprocessExisting = event.target.checked;
    });
  }

  if (refs.refreshExtractedBtn) {
    refs.refreshExtractedBtn.addEventListener("click", async () => {
      await fetchExtractedReports();
      renderExtractionWorkspace();
    });
  }

  if (refs.extractedFilterBroker) {
    refs.extractedFilterBroker.addEventListener("change", (event) => {
      state.extraction.filters.broker = event.target.value;
      renderExtractionWorkspace();
    });
  }

  if (refs.extractedFilterType) {
    refs.extractedFilterType.addEventListener("change", (event) => {
      state.extraction.filters.reportType = event.target.value;
      renderExtractionWorkspace();
    });
  }

  if (refs.extractedFilterIncludeDuplicates) {
    refs.extractedFilterIncludeDuplicates.addEventListener("change", async (event) => {
      state.extraction.filters.includeDuplicates = event.target.checked;
      await fetchExtractedReports();
      renderExtractionWorkspace();
    });
  }

  if (refs.extractedFilterCompany) {
    refs.extractedFilterCompany.addEventListener("input", () => {
      scheduleExtractionCompanySearch();
    });
  }

  if (refs.extractionRunsTable) {
    refs.extractionRunsTable.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-abort-run]");
      if (!(button instanceof HTMLButtonElement)) {
        return;
      }

      const runId = String(button.dataset.abortRun || "").trim();
      if (!runId) {
        return;
      }

      void abortExtractionRun(runId);
    });
  }

  refs.archiveBrokerFilter.addEventListener("change", (event) => {
    state.archives.brokerFilter = event.target.value;
    state.archives.page = 1;
    renderArchiveView();
  });

  refs.archiveSearch.addEventListener("input", (event) => {
    state.archives.search = event.target.value.trim().toLowerCase();
    scheduleArchiveSearch();
  });

  refs.archiveRefreshBtn.addEventListener("click", async () => {
    state.archives.page = 1;
    await fetchArchives();
    renderArchiveView();
  });

  refs.archivePrevBtn.addEventListener("click", () => {
    if (state.archives.page <= 1) {
      return;
    }

    state.archives.page -= 1;
    renderArchiveView();
  });

  refs.archiveNextBtn.addEventListener("click", () => {
    const totalPages = getArchiveTotalPages();
    if (state.archives.page >= totalPages) {
      return;
    }

    state.archives.page += 1;
    renderArchiveView();
  });

  refs.notificationsRefreshBtn.addEventListener("click", async () => {
    await fetchNotifications();
  });

  refs.notificationsExchangeSelect.addEventListener("change", async () => {
    state.notifications.page = 1;
    if (String(refs.notificationsExchangeSelect?.value ?? "").trim().toUpperCase() !== "DEDUP") {
      state.notifications.aiLabelFilter = "all";
      if (refs.notificationsAiLabelFilter) {
        refs.notificationsAiLabelFilter.value = "all";
      }
    }
    await fetchNotifications();
  });

  refs.notificationsLimitSelect.addEventListener("change", async () => {
    state.notifications.page = 1;
    await fetchNotifications();
  });

  if (refs.notificationsAiLabelFilter) {
    refs.notificationsAiLabelFilter.addEventListener("change", async (event) => {
      state.notifications.aiLabelFilter = normalizeAiLabelFilterValue(event.target.value);
      state.notifications.page = 1;
      await fetchNotifications();
    });
  }

  refs.notificationsSymbolInput.addEventListener("input", () => {
    scheduleNotificationsSearch();
  });

  refs.notificationsSymbolInput.addEventListener("keydown", async (event) => {
    if (event.key !== "Enter") {
      return;
    }

    event.preventDefault();
    state.notifications.page = 1;
    await fetchNotifications();
  });

  refs.notificationsPrevBtn.addEventListener("click", async () => {
    if (state.notifications.page <= 1) {
      return;
    }

    state.notifications.page -= 1;
    await fetchNotifications();
  });

  refs.notificationsNextBtn.addEventListener("click", async () => {
    if (state.notifications.page >= state.notifications.totalPages) {
      return;
    }

    state.notifications.page += 1;
    await fetchNotifications();
  });

  if (refs.notificationsTable) {
    refs.notificationsTable.addEventListener("click", async (event) => {
      const saveButton = event.target.closest("button[data-review-save]");
      if (!saveButton) {
        return;
      }

      const dedupAnnouncementKey = String(saveButton.dataset.reviewSave ?? "").trim();
      if (!dedupAnnouncementKey) {
        return;
      }

      const row = saveButton.closest("tr");
      const select = row?.querySelector("select[data-review-select]");
      const reviewedLabel = String(select?.value ?? "").trim();
      if (!reviewedLabel) {
        state.notifications.reviewFeedback = "Select the correct label before saving.";
        renderNotifications();
        return;
      }

      await saveNotificationReview(dedupAnnouncementKey, reviewedLabel);
    });
  }

  if (refs.notificationsSuggestionSubmitBtn) {
    refs.notificationsSuggestionSubmitBtn.addEventListener("click", async () => {
      await submitNotificationSuggestion();
    });
  }

  if (refs.notificationsSuggestionClearContextBtn) {
    refs.notificationsSuggestionClearContextBtn.addEventListener("click", () => {
      clearNotificationSuggestionContext();
      state.notifications.suggestionFeedback = "Row context cleared.";
      renderNotifications();
    });
  }

  refs.archiveTable.addEventListener("click", async (event) => {
    const shareButton = event.target.closest("button[data-share-archive]");
    if (!shareButton) {
      return;
    }

    const archiveId = shareButton.dataset.shareArchive;
    if (!archiveId) {
      return;
    }

    shareButton.disabled = true;
    try {
      const response = await apiFetch(`/api/email-archives/${archiveId}/share-links`, {
        method: "POST",
        body: JSON.stringify({ expiresHours: 24 })
      });

      const link = response.raw?.url;
      if (link) {
        await copyToClipboard(link);
        setPipelineMessage(`Share link copied for archive ${archiveId}.`);
      } else {
        setPipelineMessage(`Share link generated for archive ${archiveId}.`);
      }
    } catch (error) {
      setPipelineMessage(`Failed to create share link: ${error.message}`);
    } finally {
      shareButton.disabled = false;
    }
  });

  refs.companySelect.addEventListener("change", (event) => {
    state.companyView.selected = event.target.value;
    renderCompanyView();
  });

  refs.companySort.addEventListener("change", (event) => {
    state.companyView.sort = event.target.value;
    renderCompanyView();
  });

  refs.dictionaryForm.addEventListener("submit", (event) => {
    event.preventDefault();

    const canonical = refs.dictionaryCanonical.value.trim();
    const ticker = refs.dictionaryTicker.value.trim();
    const aliases = refs.dictionaryAliases.value
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);

    if (!canonical || !ticker || aliases.length === 0) {
      return;
    }

    state.dictionary.push({ canonical, ticker, aliases });
    persistDictionary();
    refs.dictionaryForm.reset();
    refreshAfterDictionaryChange();
    setPipelineMessage("Added dictionary mapping and refreshed company normalization.");
  });

  refs.priorityForm.addEventListener("submit", (event) => {
    event.preventDefault();
    addCompanyToList("priority", refs.priorityInput.value);
    refs.priorityForm.reset();
  });

  refs.ignoreForm.addEventListener("submit", (event) => {
    event.preventDefault();
    addCompanyToList("ignore", refs.ignoreInput.value);
    refs.ignoreForm.reset();
  });

  refs.recipientForm.addEventListener("submit", (event) => {
    event.preventDefault();
    const email = refs.recipientInput.value.trim().toLowerCase();
    if (!email || state.digestRecipients.includes(email)) {
      return;
    }

    state.digestRecipients.push(email);
    persistList(STORAGE_KEYS.recipients, state.digestRecipients);
    renderSettings();
    renderDigest();
  });

  refs.scheduleSaveBtn.addEventListener("click", () => {
    state.digestSchedule = refs.scheduleInput.value.trim() || "Daily 07:30 IST";
    localStorage.setItem(STORAGE_KEYS.schedule, state.digestSchedule);
    renderDigest();
    setPipelineMessage("Digest schedule saved.");
  });

  refs.priorityList.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-company]");
    if (!button) {
      return;
    }

    state.priorityCompanies = state.priorityCompanies.filter((value) => value !== button.dataset.company);
    persistList(STORAGE_KEYS.priority, state.priorityCompanies);
    renderAllDataViews();
  });

  refs.ignoreList.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-company]");
    if (!button) {
      return;
    }

    state.ignoredCompanies = state.ignoredCompanies.filter((value) => value !== button.dataset.company);
    persistList(STORAGE_KEYS.ignored, state.ignoredCompanies);
    renderAllDataViews();
  });

  refs.recipientList.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-recipient]");
    if (!button) {
      return;
    }

    state.digestRecipients = state.digestRecipients.filter((value) => value !== button.dataset.recipient);
    persistList(STORAGE_KEYS.recipients, state.digestRecipients);
    renderSettings();
    renderDigest();
  });
}

function renderAll() {
  renderViewState();
  renderAuthUi();
  renderIngestSetupModal();
  renderAllDataViews();
}

function renderAllDataViews() {
  renderDashboard();
  renderExtractionWorkspace();
  renderArchiveView();
  renderCompanyView();
  renderNotifications();
  renderSettings();
  renderDigest();
}

function renderViewState() {
  for (const [viewName, element] of Object.entries(refs.views)) {
    const isBrokerageView = viewName !== "notifications";
    const isActive =
      state.section === "notifications"
        ? viewName === "notifications"
        : isBrokerageView && viewName === state.view;
    element.classList.toggle("active", isActive);
  }

  for (const button of refs.sectionButtons) {
    button.classList.toggle("active", button.dataset.section === state.section);
  }

  if (refs.brokerageViewTabs) {
    refs.brokerageViewTabs.classList.toggle("hidden", state.section !== "brokerage");
  }

  const brokerageActionOnly = state.section === "brokerage";
  refs.ingestSetupBtn.classList.toggle("hidden", !brokerageActionOnly);
  refs.runIngestBtn.classList.toggle("hidden", !brokerageActionOnly);
  refs.sendDigestBtn.classList.toggle("hidden", !brokerageActionOnly);

  for (const button of refs.tabButtons) {
    button.classList.toggle("active", button.dataset.view === state.view);
  }
}

function renderAuthUi() {
  const signedIn = Boolean(state.auth.token && state.auth.user);

  if (!signedIn) {
    refs.authStatus.textContent = "Auth status: not signed in";
    refs.googleConnectBtn.textContent = "Sign in with Google";
    refs.signoutBtn.classList.add("hidden");
    refs.ingestSetupBtn.disabled = true;
    refs.runIngestBtn.disabled = true;
    return;
  }

  const gmailPart = state.auth.gmailConnected ? "Gmail connected" : "Gmail not connected";
  refs.authStatus.textContent = `Auth status: ${state.auth.user.email} (${gmailPart})`;
  refs.googleConnectBtn.textContent = state.auth.gmailConnected ? "Reconnect Google" : "Connect Gmail";
  refs.signoutBtn.classList.remove("hidden");
  refs.ingestSetupBtn.disabled = !state.auth.gmailConnected;
  refs.runIngestBtn.disabled = !state.auth.gmailConnected;
}

function renderDashboard() {
  const allReports = buildReports();
  const canonicalReports = allReports.filter((report) => !report.duplicateOf);
  const duplicateCount = allReports.length - canonicalReports.length;

  const visibleReports = allReports.filter((report) => {
    if (!state.filters.includeDuplicates && report.duplicateOf) {
      return false;
    }

    if (state.ignoredCompanies.includes(report.canonicalCompany)) {
      return false;
    }

    if (state.filters.broker !== "All" && report.broker !== state.filters.broker) {
      return false;
    }

    if (state.filters.type !== "All" && report.type !== state.filters.type) {
      return false;
    }

    if (state.filters.search) {
      const query = state.filters.search.toLowerCase();
      const haystack = `${report.canonicalCompany} ${report.summary}`.toLowerCase();
      if (!haystack.includes(query)) {
        return false;
      }
    }

    return true;
  });

  const priorityHitCount = canonicalReports.filter((report) => state.priorityCompanies.includes(report.canonicalCompany)).length;

  refs.chipRow.innerHTML = `
    ${state.priorityCompanies
      .map((company) => `<span class="chip priority">Priority: ${escapeHtml(company)}</span>`)
      .join("")}
    ${state.ignoredCompanies.map((company) => `<span class="chip ignore">Ignored: ${escapeHtml(company)}</span>`).join("")}
  `;

  refs.kpiGrid.innerHTML = `
    <article class="kpi">
      <h4>Raw Reports</h4>
      <p>${allReports.length}</p>
      <small>seed + ingested archive</small>
    </article>
    <article class="kpi">
      <h4>Canonical Reports</h4>
      <p>${canonicalReports.length}</p>
      <small>dedupe primary events</small>
    </article>
    <article class="kpi">
      <h4>Collapsed Duplicates</h4>
      <p>${duplicateCount}</p>
      <small>roundup repeats removed</small>
    </article>
    <article class="kpi">
      <h4>Priority Hits</h4>
      <p>${priorityHitCount}</p>
      <small>ranked first in digest</small>
    </article>
  `;

  const grouped = groupBy(visibleReports, "broker");
  const brokers = Object.keys(grouped).sort();

  if (brokers.length === 0) {
    refs.brokerLanes.innerHTML = `<div class="empty-state">No reports match the selected filters.</div>`;
    return;
  }

  refs.brokerLanes.innerHTML = brokers
    .map((broker, index) => {
      const cards = grouped[broker]
        .sort((a, b) => b.timestamp - a.timestamp)
        .map((report) => renderReportCard(report))
        .join("");

      return `
        <section class="lane broker-${(index % 3) + 1}">
          <header>${escapeHtml(broker)} · Distinct Broker Lane</header>
          <div class="lane-body">${cards}</div>
        </section>
      `;
    })
    .join("");
}

function renderExtractionWorkspace() {
  if (
    !refs.extractionStatusNote ||
    !refs.extractionLastRun ||
    !refs.extractionRunsTable ||
    !refs.extractedReportsMeta ||
    !refs.extractedReportsTable
  ) {
    return;
  }

  hydrateExtractedReportFilters();

  const authMissing = !state.auth.token;
  const triggerUnavailable = state.extraction.endpointStatus.trigger === "unavailable";
  const runsUnavailable = state.extraction.endpointStatus.runs === "unavailable";
  const reportsUnavailable = state.extraction.endpointStatus.reports === "unavailable";

  let statusClass = "";
  let statusMessage = state.extraction.statusMessage || "";
  if (!statusMessage) {
    statusMessage = authMissing
      ? "Sign in with Google to access extraction runs and extracted reports."
      : "Extraction workspace ready.";
  }

  if (triggerUnavailable && runsUnavailable && reportsUnavailable) {
    statusClass = "warn";
    statusMessage = "Extraction endpoints are unavailable in this backend. Workspace is in standby mode.";
  } else if (state.extraction.runsError || state.extraction.reportsError) {
    statusClass = "warn";
  } else if (state.extraction.fetchedAt && !state.extraction.runsLoading && !state.extraction.reportsLoading) {
    statusClass = "good";
  }

  refs.extractionStatusNote.className = `note${statusClass ? ` ${statusClass}` : ""}`;
  refs.extractionStatusNote.textContent = statusMessage;

  if (refs.runExtractionBtn) {
    refs.runExtractionBtn.disabled = state.extraction.runStarting || triggerUnavailable || authMissing;
    refs.runExtractionBtn.textContent = state.extraction.runStarting ? "Running extraction..." : "Run extraction now";
  }

  if (refs.refreshExtractionBtn) {
    refs.refreshExtractionBtn.disabled = state.extraction.runStarting || state.extraction.runsLoading || state.extraction.reportsLoading;
  }

  if (refs.extractionReprocessExisting) {
    refs.extractionReprocessExisting.disabled = state.extraction.runStarting;
    refs.extractionReprocessExisting.checked = state.extraction.reprocessExisting === true;
  }

  if (refs.refreshExtractedBtn) {
    refs.refreshExtractedBtn.disabled = state.extraction.reportsLoading;
  }

  const lastRun = normalizeExtractionRun(state.extraction.lastRun);
  if (state.extraction.runsLoading && !lastRun) {
    refs.extractionLastRun.className = "note";
    refs.extractionLastRun.textContent = "Last run: loading...";
  } else if (lastRun) {
    const status = formatExtractionRunStatus(lastRun.status);
    refs.extractionLastRun.className = "note";
    refs.extractionLastRun.innerHTML = `
      <strong>Last run</strong>: ${escapeHtml(formatDateTime(lastRun.startedAt))} | ${escapeHtml(status)} |
      processed ${escapeHtml(formatCount(lastRun.processedCount))},
      extracted ${escapeHtml(formatCount(lastRun.extractedCount))},
      duplicates ${escapeHtml(formatCount(lastRun.duplicatesCount))}
    `;
  } else if (runsUnavailable) {
    refs.extractionLastRun.className = "note warn";
    refs.extractionLastRun.textContent = "Last run: unavailable (extraction runs endpoint not found).";
  } else if (authMissing) {
    refs.extractionLastRun.className = "note";
    refs.extractionLastRun.textContent = "Last run: sign in required.";
  } else {
    refs.extractionLastRun.className = "note";
    refs.extractionLastRun.textContent = "Last run: no extraction runs yet.";
  }

  if (state.extraction.runsLoading && state.extraction.runs.length === 0) {
    refs.extractionRunsTable.innerHTML =
      '<tr><td colspan="6"><div class="empty-state">Loading extraction run history...</div></td></tr>';
  } else if (runsUnavailable) {
    refs.extractionRunsTable.innerHTML =
      '<tr><td colspan="6"><div class="empty-state">Extraction run history endpoint is unavailable in this environment.</div></td></tr>';
  } else if (state.extraction.runsError) {
    refs.extractionRunsTable.innerHTML = `<tr><td colspan="6"><div class="empty-state">Failed to load extraction runs: ${escapeHtml(
      state.extraction.runsError
    )}</div></td></tr>`;
  } else if (state.extraction.runs.length === 0) {
    refs.extractionRunsTable.innerHTML =
      '<tr><td colspan="6"><div class="empty-state">No extraction runs yet. Trigger a run to populate history.</div></td></tr>';
  } else {
    refs.extractionRunsTable.innerHTML = state.extraction.runs
      .map((run) => {
        const status = formatExtractionRunStatus(run.status);
        const abortable = isExtractionRunAbortable(run);
        const abortPending = run.abortPending === true;
        const aborting = state.extraction.abortingRunId === run.id;
        const disableAbort = aborting || abortPending || state.extraction.runStarting || state.extraction.runsLoading;
        const action = abortable
          ? `<button class="btn extraction-abort-btn" type="button" data-abort-run="${escapeAttribute(run.id)}"${
              disableAbort ? " disabled" : ""
            }>${aborting ? "Aborting..." : abortPending ? "Abort requested" : "Abort run"}</button>`
          : `<span class="muted-inline">-</span>`;
        return `
          <tr>
            <td>${escapeHtml(formatDateTime(run.startedAt))}</td>
            <td><span class="tag extraction-status ${escapeAttribute(extractionStatusClass(run.status))}">${escapeHtml(status)}</span></td>
            <td>${escapeHtml(formatCount(run.processedCount))}</td>
            <td>${escapeHtml(formatCount(run.extractedCount))}</td>
            <td>${escapeHtml(formatCount(run.duplicatesCount))}</td>
            <td>${action}</td>
          </tr>
        `;
      })
      .join("");
  }

  const visibleReports = getVisibleExtractedReports();
  const fetchedAtLabel = state.extraction.fetchedAt ? new Date(state.extraction.fetchedAt).toLocaleString() : "not fetched yet";
  const duplicateVisibleCount = visibleReports.filter((report) => report.duplicateOfReportId).length;

  if (reportsUnavailable) {
    refs.extractedReportsMeta.className = "note warn";
    refs.extractedReportsMeta.textContent = "Extracted report endpoint unavailable. Waiting for backend rollout.";
  } else if (state.extraction.reportsError) {
    refs.extractedReportsMeta.className = "note warn";
    refs.extractedReportsMeta.textContent = `Failed to load extracted reports: ${state.extraction.reportsError}`;
  } else {
    refs.extractedReportsMeta.className = "note";
    refs.extractedReportsMeta.textContent = `${visibleReports.length} visible extracted reports (${state.extraction.reportsTotal} loaded, ${duplicateVisibleCount} duplicates shown). Last fetch: ${fetchedAtLabel}.`;
  }

  if (state.extraction.reportsLoading && state.extraction.reports.length === 0) {
    refs.extractedReportsTable.innerHTML =
      '<tr><td colspan="9"><div class="empty-state">Loading extracted reports...</div></td></tr>';
    return;
  }

  if (reportsUnavailable) {
    refs.extractedReportsTable.innerHTML =
      '<tr><td colspan="9"><div class="empty-state">Extracted reports will appear here once extraction endpoints are available.</div></td></tr>';
    return;
  }

  if (state.extraction.reportsError) {
    refs.extractedReportsTable.innerHTML = `<tr><td colspan="9"><div class="empty-state">Failed to load extracted reports: ${escapeHtml(
      state.extraction.reportsError
    )}</div></td></tr>`;
    return;
  }

  if (state.extraction.reports.length === 0) {
    refs.extractedReportsTable.innerHTML =
      '<tr><td colspan="9"><div class="empty-state">No extracted reports yet. Run extraction to generate report records.</div></td></tr>';
    return;
  }

  if (visibleReports.length === 0) {
    refs.extractedReportsTable.innerHTML =
      '<tr><td colspan="9"><div class="empty-state">No extracted reports match the selected filters.</div></td></tr>';
    return;
  }

  refs.extractedReportsTable.innerHTML = visibleReports
    .map((report) => {
      const confidenceText = report.confidence === null ? "-" : `${Math.round(report.confidence)}%`;
      const sourceLink = buildExtractedSourceLink(report);
      const dedupeLabel = report.duplicateOfReportId ? "Duplicate" : "Canonical";
      const dedupeClass = report.duplicateOfReportId ? "warn" : "ok";
      const dedupeDetail = report.duplicateOfReportId
        ? `of ${report.duplicateOfReportId}${report.dedupeMethod ? ` (${report.dedupeMethod})` : ""}`
        : report.duplicateKey
          ? report.duplicateKey.slice(0, 16)
          : "primary";
      const insights = report.keyPoints
        .slice(0, 2)
        .map((point) => `<li>${escapeHtml(point)}</li>`)
        .join("");
      const summaryCell = `
        <div class="extracted-summary">${escapeHtml(report.summary)}</div>
        ${insights ? `<ul class="extracted-insights">${insights}</ul>` : ""}
      `;

      return `
        <tr>
          <td>${escapeHtml(formatDateTime(report.publishedAt))}</td>
          <td>${escapeHtml(report.broker)}</td>
          <td>${escapeHtml(report.companyCanonical)}</td>
          <td>${escapeHtml(report.reportType)}</td>
          <td>
            <span class="tag ${escapeAttribute(dedupeClass)}">${escapeHtml(dedupeLabel)}</span>
            <div class="dedupe-hint">${escapeHtml(dedupeDetail)}</div>
          </td>
          <td>${escapeHtml(report.title)}</td>
          <td>${summaryCell}</td>
          <td>${escapeHtml(confidenceText)}</td>
          <td>${sourceLink}</td>
        </tr>
      `;
    })
    .join("");
}

function hydrateExtractedReportFilters() {
  if (
    !refs.extractedFilterBroker ||
    !refs.extractedFilterType ||
    !refs.extractedFilterCompany ||
    !refs.extractedFilterIncludeDuplicates
  ) {
    return;
  }

  const brokers = Array.from(new Set(state.extraction.reports.map((report) => report.broker).filter(Boolean))).sort();
  refs.extractedFilterBroker.innerHTML = [
    '<option value="All">All</option>',
    ...brokers.map((broker) => `<option value="${escapeAttribute(broker)}">${escapeHtml(broker)}</option>`)
  ].join("");
  if (!brokers.includes(state.extraction.filters.broker)) {
    state.extraction.filters.broker = "All";
  }
  refs.extractedFilterBroker.value = state.extraction.filters.broker;

  const reportTypes = Array.from(new Set(state.extraction.reports.map((report) => report.reportType).filter(Boolean))).sort();
  refs.extractedFilterType.innerHTML = [
    '<option value="All">All</option>',
    ...reportTypes.map((reportType) => `<option value="${escapeAttribute(reportType)}">${escapeHtml(reportType)}</option>`)
  ].join("");
  if (!reportTypes.includes(state.extraction.filters.reportType)) {
    state.extraction.filters.reportType = "All";
  }
  refs.extractedFilterType.value = state.extraction.filters.reportType;

  if (refs.extractedFilterCompany.value !== state.extraction.filters.company) {
    refs.extractedFilterCompany.value = state.extraction.filters.company;
  }

  refs.extractedFilterIncludeDuplicates.checked = state.extraction.filters.includeDuplicates === true;
}

function getVisibleExtractedReports() {
  const brokerFilter = state.extraction.filters.broker;
  const reportTypeFilter = state.extraction.filters.reportType;
  const companyQuery = state.extraction.filters.company.trim().toLowerCase();

  return state.extraction.reports
    .filter((report) => {
      if (brokerFilter !== "All" && report.broker !== brokerFilter) {
        return false;
      }

      if (reportTypeFilter !== "All" && report.reportType !== reportTypeFilter) {
        return false;
      }

      if (companyQuery) {
        const haystack = `${report.companyCanonical} ${report.companyRaw} ${report.title}`.toLowerCase();
        if (!haystack.includes(companyQuery)) {
          return false;
        }
      }

      return true;
    })
    .sort((left, right) => right.publishedAtEpoch - left.publishedAtEpoch);
}

function buildExtractedSourceLink(report) {
  if (report.sourceUrl) {
    return `<a class="link-btn" href="${escapeAttribute(report.sourceUrl)}" target="_blank" rel="noopener">Source</a>`;
  }

  if (report.archiveId) {
    const archivePath = `/api/email-archives/${encodeURIComponent(report.archiveId)}/raw`;
    return `<a class="link-btn" href="${escapeAttribute(toApiAbsolute(archivePath))}" target="_blank" rel="noopener">Archive ${escapeHtml(
      report.archiveId
    )}</a>`;
  }

  return "-";
}

function formatExtractionRunStatus(value) {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    return "Unknown";
  }

  return normalized
    .replaceAll("-", "_")
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

function extractionStatusClass(value) {
  return `extraction-status-${String(value ?? "unknown")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "unknown"}`;
}

function isExtractionRunAbortable(run) {
  const status = String(run?.status ?? "").trim().toLowerCase();
  if (!status) {
    return false;
  }

  if (status !== "queued" && status !== "running") {
    return false;
  }

  return run?.abortPending !== true;
}

function renderReportCard(report) {
  const duplicateTag = report.duplicateOf ? `<span class="tag duplicate">Duplicate snippet</span>` : "";
  const insights = Array.isArray(report.insights) ? report.insights.slice(0, 3) : [];
  const insightsHtml =
    insights.length > 0
      ? `<ul class="report-insights">${insights.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`
      : "";

  return `
    <article class="report-card">
      <h3>${escapeHtml(report.canonicalCompany)} · ${escapeHtml(report.type)}</h3>
      <div class="meta-tags">
        <span class="tag ${tagClassForType(report.type)}">${escapeHtml(report.type)}</span>
        <span class="tag">${escapeHtml(report.coverage)}</span>
        <span class="tag">${formatTime(report.time)}</span>
        ${duplicateTag}
      </div>
      <p class="summary">${escapeHtml(report.summary)}</p>
      ${insightsHtml}
      <div class="card-actions">
        <a class="link-btn" href="${escapeAttribute(report.links.archive)}" target="_blank" rel="noopener">Open archived .eml</a>
        <a class="link-btn" href="${escapeAttribute(report.links.pdf)}" target="_blank" rel="noopener">Open attachment PDF</a>
        <a class="link-btn" href="${escapeAttribute(report.links.gmail)}" target="_blank" rel="noopener">Open Gmail thread</a>
      </div>
    </article>
  `;
}

function renderArchiveView() {
  const records = getVisibleArchives();
  const totalPages = getArchiveTotalPages(records.length);
  state.archives.page = Math.min(Math.max(state.archives.page, 1), totalPages);
  const startIndex = (state.archives.page - 1) * state.archives.pageSize;
  const pageItems = records.slice(startIndex, startIndex + state.archives.pageSize);

  const brokers = Array.from(new Set(state.archives.items.map((item) => item.broker))).sort();
  refs.archiveBrokerFilter.innerHTML = [
    `<option value="All">All</option>`,
    ...brokers.map((broker) => `<option value="${escapeAttribute(broker)}">${escapeHtml(broker)}</option>`)
  ].join("");
  refs.archiveBrokerFilter.value = state.archives.brokerFilter;

  const fetchedText = state.archives.fetchedAt ? new Date(state.archives.fetchedAt).toLocaleString() : "not fetched yet";
  refs.archiveSummary.innerHTML = `<strong>${records.length}</strong> visible archives (${state.archives.total} total loaded). Last fetch: ${escapeHtml(
    fetchedText
  )}. Page ${state.archives.page} of ${totalPages}.`;
  if (refs.archivePageLabel) {
    refs.archivePageLabel.textContent = `Page ${state.archives.page} of ${totalPages}`;
  }
  if (refs.archivePrevBtn) {
    refs.archivePrevBtn.disabled = state.archives.page <= 1;
  }
  if (refs.archiveNextBtn) {
    refs.archiveNextBtn.disabled = state.archives.page >= totalPages;
  }

  if (records.length === 0) {
    refs.archiveTable.innerHTML =
      '<tr><td colspan="5"><div class="empty-state">No archived emails yet. Sign in and run ingest to populate this table.</div></td></tr>';
    return;
  }

  refs.archiveTable.innerHTML = pageItems
    .map((item) => {
      const attachments = item.attachments
        .slice(0, 2)
        .map(
          (attachment) =>
            `<a class="link-btn" href="${escapeAttribute(toApiAbsolute(attachment.downloadUrl))}" target="_blank" rel="noopener">${escapeHtml(
              attachment.filename
            )}</a>`
        )
        .join("");

      return `
        <tr>
          <td>${escapeHtml(formatShortDate(item.ingestedAt))}</td>
          <td>${escapeHtml(item.broker)}</td>
          <td>${escapeHtml(item.from)}</td>
          <td>
            <strong>${escapeHtml(item.subject)}</strong>
            <br />
            <small>${escapeHtml(item.snippet || item.bodyPreview || "")}</small>
          </td>
          <td>
            <div class="archive-links">
              <a class="link-btn" href="${escapeAttribute(toApiAbsolute(item.downloadUrl))}" target="_blank" rel="noopener">Open .eml</a>
              ${attachments}
              <a class="link-btn" href="${escapeAttribute(item.gmailMessageUrl || "#")}" target="_blank" rel="noopener">Gmail</a>
              <button class="btn" data-share-archive="${escapeAttribute(item.id)}" type="button">Share link</button>
            </div>
          </td>
        </tr>
      `;
    })
    .join("");
}

function renderCompanyView() {
  const reports = buildReports().filter(
    (report) => report.canonicalCompany === state.companyView.selected && !state.ignoredCompanies.includes(report.canonicalCompany)
  );

  const sorted =
    state.companyView.sort === "broker"
      ? [...reports].sort((a, b) => a.broker.localeCompare(b.broker))
      : [...reports].sort((a, b) => b.timestamp - a.timestamp);

  if (sorted.length === 0) {
    refs.companyTimeline.innerHTML = `<div class="empty-state">No visible reports for this company.</div>`;
  } else {
    refs.companyTimeline.innerHTML = sorted
      .map(
        (report) => `
          <article class="item">
            <h3>${escapeHtml(report.broker)} · ${escapeHtml(report.type)} · ${formatShortDate(report.time)}</h3>
            <div class="meta-tags">
              <span class="tag ${tagClassForType(report.type)}">${escapeHtml(report.type)}</span>
              <span class="tag">${report.duplicateOf ? "Duplicate Reference" : "Canonical"}</span>
            </div>
            <p class="summary">${escapeHtml(report.summary)}</p>
            <div class="card-actions">
              <a class="link-btn" href="${escapeAttribute(report.links.archive)}" target="_blank" rel="noopener">Open archived .eml</a>
              <a class="link-btn" href="${escapeAttribute(report.links.pdf)}" target="_blank" rel="noopener">Open PDF</a>
            </div>
          </article>
        `
      )
      .join("");
  }

  const canonical = reports.filter((report) => !report.duplicateOf);
  const duplicates = reports.filter((report) => report.duplicateOf);
  const sentiment = {
    bullish: canonical.filter((report) => report.sentiment === "bullish").length,
    neutral: canonical.filter((report) => report.sentiment === "neutral").length,
    bearish: canonical.filter((report) => report.sentiment === "bearish").length
  };

  refs.companyWorkbench.innerHTML = `
    <div class="workbench-list">
      <div class="note good"><strong>Signal map:</strong> ${sentiment.bullish} bullish, ${sentiment.neutral} neutral, ${sentiment.bearish} bearish.</div>
      <div class="note"><strong>Canonical references:</strong> ${canonical.length}<br /><strong>Collapsed duplicates:</strong> ${duplicates.length}</div>
      <div class="note"><strong>Priority status:</strong> ${state.priorityCompanies.includes(state.companyView.selected) ? "In priority list" : "Not in priority list"}</div>
      <div class="note warn"><strong>Broker separation:</strong> summaries remain broker-native and are not merged.</div>
      <div class="note"><strong>Source access:</strong> archived .eml and attachment links are shareable with signed URLs.</div>
    </div>
  `;
}

function renderSettings() {
  refs.dictionaryTable.innerHTML = state.dictionary
    .map(
      (entry) => `
        <tr>
          <td>${escapeHtml(entry.canonical)}</td>
          <td>${escapeHtml(entry.ticker)}</td>
          <td>${escapeHtml(entry.aliases.join(", "))}</td>
          <td><span class="tag">Mapped</span></td>
        </tr>
      `
    )
    .join("");

  refs.priorityList.innerHTML = renderRemovableChipList(state.priorityCompanies, "priority", "data-company");
  refs.ignoreList.innerHTML = renderRemovableChipList(state.ignoredCompanies, "ignore", "data-company");
  refs.recipientList.innerHTML = renderRemovableChipList(state.digestRecipients, "", "data-recipient");
  refs.scheduleInput.value = state.digestSchedule;
}

function renderDigestFromApiPreview(preview) {
  const sections = Array.isArray(preview?.sections) ? preview.sections : [];
  const generatedAt = preview?.generatedAt ? new Date(preview.generatedAt).toLocaleString() : "unknown";
  const totalReports = Number(preview?.totalReports || 0);
  const duplicateSuppressed = Number(preview?.duplicateSuppressed || 0);
  const priorityHits = Number(preview?.priorityHits || 0);

  return `
    <div class="digest-block">
      <h3>Delivery Target</h3>
      <p><strong>Recipients:</strong> ${escapeHtml(state.digestRecipients.join(", ") || "none")}</p>
      <p><strong>Schedule:</strong> ${escapeHtml(state.digestSchedule)}</p>
      <p><strong>Generated:</strong> ${escapeHtml(generatedAt)}</p>
      <p><strong>Status:</strong> ${state.digestApi.loading ? "sending..." : state.digestApi.lastSentAt ? "last sent" : "preview ready"}</p>
    </div>
    <div class="digest-block">
      <h3>Digest Stats</h3>
      <p><strong>Total reports:</strong> ${totalReports}</p>
      <p><strong>Priority hits:</strong> ${priorityHits}</p>
      <p><strong>Duplicates suppressed:</strong> ${duplicateSuppressed}</p>
    </div>
    ${sections
      .map((section) => {
        const items = Array.isArray(section.items) ? section.items : [];
        const listHtml = items
          .map(
            (item) =>
              `<li><strong>${escapeHtml(item.companyCanonical)} · ${escapeHtml(item.reportType)}:</strong> ${escapeHtml(item.summary)}</li>`
          )
          .join("");
        return `
          <div class="digest-block">
            <h3>${escapeHtml(section.broker || "Unmapped Broker")}</h3>
            <ul class="digest-list">${listHtml || "<li>No items</li>"}</ul>
          </div>
        `;
      })
      .join("")}
    ${state.digestApi.error ? `<div class="message-toast">${escapeHtml(state.digestApi.error)}</div>` : ""}
    ${state.pipelineMessage ? `<div class="message-toast">${escapeHtml(state.pipelineMessage)}</div>` : ""}
  `;
}

function renderDigest() {
  if (state.digestApi.preview) {
    refs.digestPreview.innerHTML = renderDigestFromApiPreview(state.digestApi.preview);
    return;
  }

  const reports = buildReports()
    .filter((report) => !report.duplicateOf)
    .filter((report) => !state.ignoredCompanies.includes(report.canonicalCompany));

  const priorityOrder = state.priorityCompanies
    .map((company) => ({
      company,
      count: reports.filter((report) => report.canonicalCompany === company).length
    }))
    .filter((entry) => entry.count > 0);

  const byBroker = groupBy(reports, "broker");
  const duplicateCount = buildReports().filter((report) => report.duplicateOf).length;

  refs.digestPreview.innerHTML = `
    <div class="digest-block">
      <h3>Delivery Target</h3>
      <p><strong>Recipients:</strong> ${escapeHtml(state.digestRecipients.join(", ") || "none")}</p>
      <p><strong>Schedule:</strong> ${escapeHtml(state.digestSchedule)}</p>
    </div>

    <div class="digest-block">
      <h3>Priority Queue First</h3>
      <p>${
        priorityOrder.length > 0
          ? priorityOrder.map((entry) => `${escapeHtml(entry.company)} (${entry.count})`).join(" -> ")
          : "No priority companies in the current report set."
      }</p>
    </div>

    ${Object.keys(byBroker)
      .sort()
      .map((broker) => {
        const list = byBroker[broker]
          .sort((a, b) => b.timestamp - a.timestamp)
          .map(
            (report) =>
              `<li><strong>${escapeHtml(report.canonicalCompany)} · ${escapeHtml(report.type)}:</strong> ${escapeHtml(report.summary)}</li>`
          )
          .join("");

        return `
          <div class="digest-block">
            <h3>${escapeHtml(broker)}</h3>
            <ul class="digest-list">${list}</ul>
          </div>
        `;
      })
      .join("")}

    <div class="digest-block">
      <h3>Dedupe Audit</h3>
      <p>${duplicateCount} duplicate snippets are suppressed from canonical digest generation.</p>
    </div>

    ${state.pipelineMessage ? `<div class="message-toast">${escapeHtml(state.pipelineMessage)}</div>` : ""}
  `;
}

function buildDigestPreferencePayload() {
  return {
    priorityCompanies: [...state.priorityCompanies],
    ignoredCompanies: [...state.ignoredCompanies],
    limit: 250
  };
}

async function fetchDigestPreviewFromApi() {
  if (!state.auth.token) {
    state.digestApi.preview = null;
    state.digestApi.error = "Sign in with Google to load digest from extracted reports.";
    renderDigest();
    return null;
  }

  state.digestApi.loading = true;
  state.digestApi.error = "";
  renderDigest();
  try {
    const prefs = buildDigestPreferencePayload();
    const params = new URLSearchParams();
    params.set("priorityCompanies", prefs.priorityCompanies.join(","));
    params.set("ignoredCompanies", prefs.ignoredCompanies.join(","));
    params.set("limit", String(prefs.limit));
    const preview = await apiFetch(`/api/digest/preview?${params.toString()}`);
    state.digestApi.preview = preview;
    state.digestApi.error = "";
    return preview;
  } catch (error) {
    state.digestApi.preview = null;
    state.digestApi.error = error instanceof Error ? error.message : "Failed to load digest preview";
    return null;
  } finally {
    state.digestApi.loading = false;
    renderDigest();
  }
}

async function sendDigestPreviewNow() {
  state.section = "brokerage";
  state.view = "digest";
  renderViewState();

  if (!state.auth.token) {
    setPipelineMessage("Sign in with Google before sending digest.");
    state.digestApi.error = "Digest send requires Google sign-in.";
    renderDigest();
    return;
  }

  if (state.digestRecipients.length === 0) {
    setPipelineMessage("Add at least one digest recipient in Settings.");
    state.digestApi.error = "No digest recipients configured.";
    renderDigest();
    return;
  }

  const preview = await fetchDigestPreviewFromApi();
  if (!preview) {
    setPipelineMessage("Digest preview failed. Fix the error and retry.");
    return;
  }

  state.digestApi.loading = true;
  state.digestApi.error = "";
  renderDigest();

  try {
    const prefs = buildDigestPreferencePayload();
    const payload = await apiFetch("/api/digest/send", {
      method: "POST",
      body: JSON.stringify({
        recipients: [...state.digestRecipients],
        priorityCompanies: prefs.priorityCompanies,
        ignoredCompanies: prefs.ignoredCompanies,
        limit: prefs.limit
      })
    });
    state.digestApi.lastSentAt = new Date().toISOString();
    setPipelineMessage(
      `Digest sent to ${payload.recipients?.length || state.digestRecipients.length} recipients (reports: ${
        payload.digest?.totalReports ?? 0
      }, duplicates suppressed: ${payload.digest?.duplicateSuppressed ?? 0}).`
    );
  } catch (error) {
    state.digestApi.error = error instanceof Error ? error.message : "Digest send failed";
    setPipelineMessage(`Digest send failed: ${state.digestApi.error}`);
  } finally {
    state.digestApi.loading = false;
    renderDigest();
  }
}

function renderNotifications() {
  if (!refs.notificationsTable || !refs.notificationsMeta) {
    return;
  }

  if (refs.notificationsExchangeSelect) {
    const normalizedExchange = state.notifications.exchange === "BSE+NSE" ? "NSE+BSE" : state.notifications.exchange;
    refs.notificationsExchangeSelect.value = normalizedExchange;
  }
  if (refs.notificationsLimitSelect) {
    refs.notificationsLimitSelect.value = String(state.notifications.limit || 50);
  }

  const isDedupView = String(state.notifications.exchange ?? "").toUpperCase() === "DEDUP";
  if (!isDedupView) {
    state.notifications.aiLabelFilter = "all";
  }
  const normalizedAiLabelFilter = normalizeAiLabelFilterValue(state.notifications.aiLabelFilter);
  const aiFilterOptions = [
    { value: "all", label: "All AI labels" },
    { value: "pending", label: "Pending (no AI label)" },
    { value: "failed", label: "Failed" },
    { value: "reviewed", label: "Reviewed (manual label)" },
    ...getNotificationAiCategories().map((category) => ({
      value: category,
      label: formatAiCategoryLabel(category)
    }))
  ];
  if (refs.notificationsAiLabelFilter) {
    refs.notificationsAiLabelFilter.innerHTML = aiFilterOptions
      .filter((option, index, items) => items.findIndex((item) => item.value === option.value) === index)
      .map((option) => `<option value="${escapeAttribute(option.value)}">${escapeHtml(option.label)}</option>`)
      .join("");
    refs.notificationsAiLabelFilter.value = normalizedAiLabelFilter;
    if (refs.notificationsAiLabelFilter.value !== normalizedAiLabelFilter) {
      refs.notificationsAiLabelFilter.value = "all";
      state.notifications.aiLabelFilter = "all";
    }
    refs.notificationsAiLabelFilter.disabled = !isDedupView;
  }

  if (refs.notificationsReviewFeedback) {
    const defaultMessage = "Select the correct label for a row, then click Save.";
    refs.notificationsReviewFeedback.textContent = isDedupView ? state.notifications.reviewFeedback || defaultMessage : "";
  }

  const stats = state.notifications.lastSyncStats;
  const syncedLabel = state.notifications.lastSyncAt
    ? new Date(state.notifications.lastSyncAt).toLocaleString()
    : "not synced yet";

  const details = [
    `exchange: ${state.notifications.exchange}`,
    `${state.notifications.total} total announcements`,
    `showing ${state.notifications.items.length}`,
    `page ${state.notifications.page} of ${state.notifications.totalPages}`,
    `last sync: ${syncedLabel}`
  ];
  if (isDedupView && normalizedAiLabelFilter !== "all") {
    const filterLabel =
      normalizedAiLabelFilter === "pending" || normalizedAiLabelFilter === "failed" || normalizedAiLabelFilter === "reviewed"
        ? normalizedAiLabelFilter
        : formatAiCategoryLabel(normalizedAiLabelFilter);
    details.push(`ai filter: ${filterLabel}`);
  }

  if (state.notifications.source) {
    details.push(`source: ${state.notifications.source}`);
  }

  if (stats?.fromDate && stats?.toDate) {
    details.push(`range: ${stats.fromDate} to ${stats.toDate}`);
  }

  if (state.notifications.exchange === "NSE+BSE" && stats) {
    if (Number.isFinite(stats.inputCount)) {
      details.push(`source records: ${stats.inputCount}`);
    }

    if (Number.isFinite(stats.sourceDedupedCount)) {
      details.push(`source duplicates removed: ${stats.sourceDedupedCount}`);
    }
  }

  if (state.notifications.exchange === "DEDUP" && stats) {
    if (Number.isFinite(stats.inputCount)) {
      details.push(`deduped: ${state.notifications.total} unique from ${stats.inputCount} source records`);
    }

    if (Number.isFinite(stats.dedupCount)) {
      details.push(`duplicates removed: ${stats.dedupCount}`);
    }

    if (Number.isFinite(stats.withPdfHashCount)) {
      details.push(`hashed: ${stats.withPdfHashCount}`);
    }

  }

  refs.notificationsMeta.textContent = details.join(" | ");
  if (refs.notificationsPageLabel) {
    refs.notificationsPageLabel.textContent = `Page ${state.notifications.page} of ${state.notifications.totalPages}`;
  }
  if (refs.notificationsPrevBtn) {
    refs.notificationsPrevBtn.disabled = state.notifications.loading || state.notifications.page <= 1;
  }
  if (refs.notificationsNextBtn) {
    refs.notificationsNextBtn.disabled =
      state.notifications.loading ||
      state.notifications.page >= state.notifications.totalPages ||
      state.notifications.total === 0;
  }

  if (state.notifications.loading) {
    refs.notificationsTable.innerHTML = '<tr><td colspan="10"><div class="empty-state">Loading announcements...</div></td></tr>';
    return;
  }

  if (state.notifications.error) {
    refs.notificationsTable.innerHTML = `<tr><td colspan="10"><div class="empty-state">Failed to load announcements: ${escapeHtml(
      state.notifications.error
    )}</div></td></tr>`;
    return;
  }

  if (state.notifications.items.length === 0) {
    refs.notificationsTable.innerHTML =
      '<tr><td colspan="10"><div class="empty-state">No announcements found for the selected filter.</div></td></tr>';
    return;
  }

  refs.notificationsTable.innerHTML = state.notifications.items
    .map((item) => {
      const selectedExchange = String(state.notifications.exchange ?? "NSE").toUpperCase();
      const exchange = String(item.exchange ?? selectedExchange).toUpperCase();
      const combinedView =
        selectedExchange === "NSE+BSE" ||
        selectedExchange === "DEDUP" ||
        exchange === "NSE+BSE" ||
        exchange === "BSE+NSE" ||
        exchange === "COMBINED" ||
        exchange === "DEDUP";

      let exchangeLabel = exchange;
      let timestamp = "-";
      let symbol = "-";
      let company = "-";
      let type = "-";
      let attachmentUrl = null;
      let aiLabel = "-";
      let aiNotes = "-";
      let reviewColumn = "-";
      let reviewActionColumn = "-";

      if (combinedView) {
        const sourceExchanges = Array.isArray(item.exchanges)
          ? item.exchanges.map((value) => String(value).toUpperCase()).filter(Boolean)
          : [];
        const orderedExchanges = [...sourceExchanges].sort((left, right) => {
          if (left === right) {
            return 0;
          }
          if (left === "NSE") {
            return -1;
          }
          if (right === "NSE") {
            return 1;
          }
          return left.localeCompare(right);
        });
        exchangeLabel =
          orderedExchanges.length > 0
            ? orderedExchanges.join("+")
            : exchange === "COMBINED"
              ? "NSE+BSE"
              : exchange;
        if (exchangeLabel === "BSE+NSE") {
          exchangeLabel = "NSE+BSE";
        }
        timestamp = item.timestamp || "-";
        symbol = item.symbol || "-";
        company = item.company || "-";
        type = item.type || "-";
        attachmentUrl = item.attachment_url || null;
        const normalizedExchange = String(state.notifications.exchange || "").toUpperCase();
        const aiStatus = String(item.ai_status || "").toUpperCase();
        if (normalizedExchange === "DEDUP") {
          if (aiStatus === "SUCCESS" && item.ai_label) {
            const confidence = Number(item.ai_confidence);
            const aiLabelText = formatAiCategoryLabel(item.ai_label);
            aiLabel = Number.isFinite(confidence) ? `${aiLabelText} (${Math.round(confidence * 100)}%)` : aiLabelText;
            aiNotes = item.ai_reason || "-";
          } else if (aiStatus === "FAILED") {
            aiLabel = "Failed";
            const failureType = String(item.ai_failure_type || "").trim();
            const failureMessage = item.ai_error || item.ai_reason || "Unknown AI classification error";
            aiNotes = failureType ? `${failureType}: ${failureMessage}` : failureMessage;
          } else if (aiStatus === "MISSING") {
            aiLabel = "Pending";
            aiNotes = "Awaiting AI classification";
          }
        }

        const mergedCount = Number.parseInt(String(item.mergedFromCount ?? "0"), 10);
        if (Number.isFinite(mergedCount) && mergedCount > 1) {
          type = `${type} (merged ${mergedCount}x)`;
        }
      } else if (exchange === "BSE") {
        exchangeLabel = "BSE";
        timestamp = item.datetime || item.news_date || "-";
        symbol = item.scrip_code || "-";
        company = item.company_name || "-";
        type = item.category || item.subcategory || item.subject || "-";
        attachmentUrl = item.attachment_url || item.attachment_url_fallback || null;
      } else {
        exchangeLabel = "NSE";
        timestamp = item.an_dt || item.exchdisstime || "-";
        symbol = item.symbol || "-";
        company = item.sm_name || "-";
        type = item.desc || "-";
        attachmentUrl = item.attchmntfile || null;
      }

      const attachment = attachmentUrl
        ? `<a class="link-btn" href="${escapeAttribute(attachmentUrl)}" target="_blank" rel="noopener">Open</a>`
        : "-";
      const timestampLabel = formatNotificationTimestamp(timestamp);
      const aiNotesText = String(aiNotes || "-");
      const aiNotesDisplay = aiNotesText.length > 180 ? `${aiNotesText.slice(0, 177)}...` : aiNotesText;

      if (isDedupView) {
        const dedupAnnouncementKey = getDedupAnnouncementKey(item);
        const currentReviewLabel =
          String(item.review_label ?? (item.ai_status === "SUCCESS" ? item.ai_label : "") ?? "").trim();
        const categoryOptions = getNotificationAiCategories()
          .map((category) => {
            const selected = currentReviewLabel === category ? ' selected="selected"' : "";
            return `<option value="${escapeAttribute(category)}"${selected}>${escapeHtml(formatAiCategoryLabel(category))}</option>`;
          })
          .join("");
        const selectDisabled = dedupAnnouncementKey ? "" : " disabled";
        reviewColumn = dedupAnnouncementKey
          ? `<select class="review-label-select" data-review-select${selectDisabled}>
              <option value="">Select label</option>
              ${categoryOptions}
            </select>`
          : "-";

        if (dedupAnnouncementKey) {
          const submitDisabled = state.notifications.reviewSaving;
          const isSavingRow = state.notifications.reviewSaving && state.notifications.reviewSavingKey === dedupAnnouncementKey;
          const statusText = item.review_label ? "Saved" : "";
          reviewActionColumn = `
            <div class="review-row-actions">
              <button class="btn" type="button" data-review-save="${escapeAttribute(dedupAnnouncementKey)}"${
                submitDisabled ? " disabled" : ""
              }>${isSavingRow ? "Saving..." : "Save"}</button>
              ${statusText ? `<span class="review-row-status">${escapeHtml(statusText)}</span>` : ""}
            </div>
          `;
        }
      }

      return `
        <tr>
          <td>${escapeHtml(timestampLabel)}</td>
          <td>${escapeHtml(exchangeLabel)}</td>
          <td>${escapeHtml(symbol)}</td>
          <td>${escapeHtml(company)}</td>
          <td>${escapeHtml(type)}</td>
          <td>${escapeHtml(aiLabel)}</td>
          <td title="${escapeAttribute(aiNotesText)}">${escapeHtml(aiNotesDisplay)}</td>
          <td>${reviewColumn}</td>
          <td>${reviewActionColumn}</td>
          <td>${attachment}</td>
        </tr>
      `;
    })
    .join("");
}

function hydrateBrokerFilter() {
  const brokers = Array.from(new Set(buildReports().map((report) => report.broker))).sort();
  refs.filterBroker.innerHTML = [`<option value="All">All</option>`, ...brokers.map((broker) => `<option>${escapeHtml(broker)}</option>`)].join("");
}

function hydrateCompanySelect() {
  const companies = Array.from(new Set(buildReports().map((report) => report.canonicalCompany))).sort();
  refs.companySelect.innerHTML = companies.map((company) => `<option value="${escapeAttribute(company)}">${escapeHtml(company)}</option>`).join("");

  if (!companies.includes(state.companyView.selected)) {
    state.companyView.selected = companies[0] || "";
  }

  refs.companySelect.value = state.companyView.selected;
}

function buildReports() {
  const dictionaryMap = new Map();
  for (const entry of state.dictionary) {
    dictionaryMap.set(normalizeKey(entry.canonical), entry.canonical);
    for (const alias of entry.aliases) {
      dictionaryMap.set(normalizeKey(alias), entry.canonical);
    }
  }

  if (state.extraction.reports.length > 0) {
    const archiveById = new Map(state.archives.items.map((entry) => [entry.id, entry]));
    return state.extraction.reports.map((report) => {
      const archive = archiveById.get(report.archiveId) || null;
      const reportTypeLabel = String(report.reportType || "general_update")
        .replace(/[_-]+/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .replace(/\b\w/g, (value) => value.toUpperCase());
      const summary = String(report.summary || "").trim() || report.keyPoints[0] || "(No summary)";

      return normalizeReport(
        {
          id: `extracted-${report.archiveId || report.title}-${report.publishedAt || ""}`,
          broker: report.broker || "Unmapped Broker",
          company: report.companyCanonical || report.companyRaw || "Unknown Company",
          type: reportTypeLabel || "General Update",
          coverage: report.duplicateOfReportId ? "Extracted Duplicate" : "Extracted Canonical",
          time: report.publishedAt || new Date().toISOString(),
          summary,
          insights: report.keyPoints.slice(0, 3),
          sentiment: "neutral",
          duplicateOf: report.duplicateOfReportId || null,
          links: {
            archive: report.archiveId ? toApiAbsolute(`/api/email-archives/${report.archiveId}/raw`) : "#",
            pdf:
              archive?.attachments?.[0]?.downloadUrl
                ? toApiAbsolute(archive.attachments[0].downloadUrl)
                : "#",
            gmail: archive?.gmailMessageUrl || "#"
          }
        },
        dictionaryMap
      );
    });
  }

  const normalizedSeed = seedReports.map((report) => normalizeReport(report, dictionaryMap));
  const normalizedArchive = state.archives.items.map((archive) => {
    const companyGuess = guessCompanyFromSubject(archive.subject);
    const reportType = classifyReportType(archive.subject, archive.bodyPreview || archive.snippet || "");

    return normalizeReport(
      {
        id: `archive-${archive.id}`,
        broker: archive.broker || "Unmapped Broker",
        company: companyGuess,
        type: reportType,
        coverage: "Email Archive",
        time: archive.dateHeader || archive.ingestedAt,
        summary: archive.bodyPreview || archive.snippet || "(No preview)",
        insights: [],
        sentiment: "neutral",
        links: {
          archive: toApiAbsolute(archive.downloadUrl),
          pdf: archive.attachments?.[0] ? toApiAbsolute(archive.attachments[0].downloadUrl) : "#",
          gmail: archive.gmailMessageUrl || "#"
        }
      },
      dictionaryMap
    );
  });

  return [...normalizedSeed, ...normalizedArchive];
}

function normalizeReport(report, dictionaryMap) {
  const canonicalCompany = dictionaryMap.get(normalizeKey(report.company)) ?? report.company;
  return {
    ...report,
    canonicalCompany,
    duplicateOf: report.duplicateOf ?? null,
    timestamp: new Date(report.time).getTime()
  };
}

function guessCompanyFromSubject(subject) {
  const source = (subject || "").trim();
  if (!source) {
    return "Unclassified Company";
  }

  const separators = ["|", "-", "–", ":"];
  for (const separator of separators) {
    if (source.includes(separator)) {
      return source.split(separator)[0].trim();
    }
  }

  return source.split(" ").slice(0, 3).join(" ").trim();
}

function classifyReportType(subject, bodyPreview) {
  const text = `${subject} ${bodyPreview}`.toLowerCase();
  if (text.includes("initiat")) {
    return "Initiation";
  }
  if (text.includes("result") || text.includes("q1") || text.includes("q2") || text.includes("q3") || text.includes("q4")) {
    return "Results Update";
  }
  if (text.includes("sector") || text.includes("weekly") || text.includes("monitor")) {
    return "Sector Update";
  }
  return "General Update";
}

function refreshAfterDictionaryChange() {
  persistDictionary();
  hydrateBrokerFilter();
  hydrateCompanySelect();
  renderAllDataViews();
}

async function checkBackendStatus() {
  refs.backendStatus.textContent = "Backend status: checking...";

  try {
    const response = await fetch(`${getApiBase()}/api/health`);
    if (!response.ok) {
      refs.backendStatus.textContent = `Backend status: failed (HTTP ${response.status})`;
      return;
    }

    const data = await response.json();
    refs.backendStatus.textContent = `Backend status: connected (${data.env})`;
  } catch {
    refs.backendStatus.textContent = "Backend status: unreachable";
  }
}

function parseScheduleTime(value) {
  const match = String(value || "").match(/^(\d{1,2}):(\d{2})$/);
  if (!match) {
    return { hour: 7, minute: 30 };
  }

  const hour = Math.max(0, Math.min(23, Number.parseInt(match[1], 10)));
  const minute = Math.max(0, Math.min(59, Number.parseInt(match[2], 10)));
  return { hour, minute };
}

function normalizeDateInput(value) {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return "";
  }

  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return "";
  }

  const parsed = new Date(`${raw}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }

  if (parsed.toISOString().slice(0, 10) !== raw) {
    return "";
  }

  return raw;
}

function formatScheduleTime(hour, minute) {
  const safeHour = Math.max(0, Math.min(23, Number.parseInt(String(hour ?? 0), 10) || 0));
  const safeMinute = Math.max(0, Math.min(59, Number.parseInt(String(minute ?? 0), 10) || 0));
  return `${String(safeHour).padStart(2, "0")}:${String(safeMinute).padStart(2, "0")}`;
}

function closeIngestSetupModal() {
  state.ingestSetup.open = false;
  renderIngestSetupModal();
}

async function loadIngestSetupData(fetchLabels = true) {
  const prefs = await apiFetch("/api/gmail/preferences");
  const labelsPayload = fetchLabels ? await apiFetch("/api/gmail/labels") : { labels: state.ingestSetup.labels };
  const labels = Array.isArray(labelsPayload.labels) ? labelsPayload.labels : [];
  const trackedIds = Array.isArray(prefs.trackedLabelIds) ? prefs.trackedLabelIds : [];

  state.ingestSetup.labels = labels;
  state.ingestSetup.selectedLabelIds = labels
    .map((label) => label.id)
    .filter((id) => trackedIds.includes(id));
  state.ingestSetup.query = String(prefs.query || "");
  state.ingestSetup.maxResults = Number(prefs.maxResults || 30);
  state.ingestSetup.scheduleEnabled = prefs.scheduleEnabled !== false;
  state.ingestSetup.scheduleTime = formatScheduleTime(prefs.scheduleHour, prefs.scheduleMinute);
  state.ingestSetup.scheduleTimezone = String(
    prefs.scheduleTimezone || Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Kolkata"
  );
  state.ingestSetup.dateFrom = normalizeDateInput(prefs.ingestFromDate);
  state.ingestSetup.dateTo = normalizeDateInput(prefs.ingestToDate);
  state.ingestSetup.statusMessage = `Loaded ${labels.length} labels. ${trackedIds.length} currently tracked.`;
}

function renderIngestSetupModal() {
  const isOpen = state.ingestSetup.open;
  refs.ingestSetupModal.classList.toggle("hidden", !isOpen);
  if (!isOpen) {
    return;
  }

  refs.ingestSetupQuery.value = state.ingestSetup.query;
  refs.ingestSetupMaxResults.value = String(state.ingestSetup.maxResults || 30);
  refs.ingestSetupDateFrom.value = state.ingestSetup.dateFrom || "";
  refs.ingestSetupDateTo.value = state.ingestSetup.dateTo || "";
  refs.ingestSetupTime.value = state.ingestSetup.scheduleTime || "07:30";
  refs.ingestSetupTimezone.value = state.ingestSetup.scheduleTimezone || "Asia/Kolkata";
  refs.ingestSetupStartNow.checked = state.ingestSetup.startFromNow;
  refs.ingestSetupResetCursor.checked = state.ingestSetup.resetCursor;
  refs.ingestSetupSaveBtn.disabled = state.ingestSetup.saving || state.ingestSetup.loading;
  if (refs.ingestSetupResetDataBtn) {
    refs.ingestSetupResetDataBtn.disabled = state.ingestSetup.saving || state.ingestSetup.loading;
  }
  refs.ingestSetupRefreshLabelsBtn.disabled = state.ingestSetup.saving || state.ingestSetup.loading;

  if (state.ingestSetup.loading) {
    refs.ingestSetupLabels.innerHTML = '<div class="note">Loading labels...</div>';
  } else if (state.ingestSetup.labels.length === 0) {
    refs.ingestSetupLabels.innerHTML = '<div class="note">No labels found yet. Use "Refresh labels".</div>';
  } else {
    refs.ingestSetupLabels.innerHTML = state.ingestSetup.labels
      .map((label) => {
        const checked = state.ingestSetup.selectedLabelIds.includes(label.id) ? "checked" : "";
        const count = Number(label.messagesTotal || 0);
        return `
          <label class="ingest-label-item">
            <span class="left">
              <input type="checkbox" data-label-id="${escapeAttribute(label.id)}" ${checked} />
              <span>${escapeHtml(label.name)}</span>
            </span>
            <span class="meta">${escapeHtml(label.type)} · ${count} msgs</span>
          </label>
        `;
      })
      .join("");
  }

  refs.ingestSetupStatus.textContent = state.ingestSetup.statusMessage || "";
}

async function openIngestSetupModal() {
  if (!state.auth.token) {
    setPipelineMessage("Sign in with Google before configuring ingest.");
    return;
  }

  state.ingestSetup.open = true;
  state.ingestSetup.loading = true;
  state.ingestSetup.statusMessage = "Loading Gmail labels and ingest preferences...";
  renderIngestSetupModal();

  try {
    await loadIngestSetupData(true);
  } catch (error) {
    state.ingestSetup.statusMessage = `Failed to load setup: ${error.message}`;
  } finally {
    state.ingestSetup.loading = false;
    renderIngestSetupModal();
  }
}

async function refreshIngestSetupLabelsOnly() {
  if (!state.auth.token) {
    return;
  }
  state.ingestSetup.loading = true;
  state.ingestSetup.statusMessage = "Refreshing labels...";
  renderIngestSetupModal();

  try {
    await loadIngestSetupData(true);
  } catch (error) {
    state.ingestSetup.statusMessage = `Failed to refresh labels: ${error.message}`;
  } finally {
    state.ingestSetup.loading = false;
    renderIngestSetupModal();
  }
}

async function saveIngestSetup() {
  if (!state.auth.token) {
    return;
  }

  const selectedLabelIds = [...new Set(state.ingestSetup.selectedLabelIds)];
  if (selectedLabelIds.length === 0) {
    state.ingestSetup.statusMessage = "Select at least one label to track.";
    renderIngestSetupModal();
    return;
  }

  const selectedLabelNames = state.ingestSetup.labels
    .filter((label) => selectedLabelIds.includes(label.id))
    .map((label) => label.name);
  const query = refs.ingestSetupQuery.value.trim();
  const maxResults = Math.max(1, Math.min(100, Number(refs.ingestSetupMaxResults.value || state.ingestSetup.maxResults || 30)));
  const scheduleTime = parseScheduleTime(refs.ingestSetupTime.value);
  const scheduleTimezone = refs.ingestSetupTimezone.value.trim() || "Asia/Kolkata";
  const ingestFromDate = normalizeDateInput(refs.ingestSetupDateFrom.value);
  const ingestToDate = normalizeDateInput(refs.ingestSetupDateTo.value);
  const startFromNow = refs.ingestSetupStartNow.checked;
  const resetCursor = refs.ingestSetupResetCursor.checked;
  if (ingestFromDate && ingestToDate && ingestFromDate > ingestToDate) {
    state.ingestSetup.statusMessage = "Ingest start date cannot be after end date.";
    renderIngestSetupModal();
    return;
  }

  state.ingestSetup.query = query;
  state.ingestSetup.maxResults = maxResults;
  state.ingestSetup.scheduleTime = formatScheduleTime(scheduleTime.hour, scheduleTime.minute);
  state.ingestSetup.scheduleTimezone = scheduleTimezone;
  state.ingestSetup.dateFrom = ingestFromDate;
  state.ingestSetup.dateTo = ingestToDate;
  state.ingestSetup.startFromNow = startFromNow;
  state.ingestSetup.resetCursor = resetCursor;

  state.ingestSetup.saving = true;
  state.ingestSetup.statusMessage = "Saving ingest setup...";
  renderIngestSetupModal();

  try {
    await apiFetch("/api/gmail/preferences", {
      method: "PUT",
      body: JSON.stringify({
        query,
        maxResults,
        trackedLabelIds: selectedLabelIds,
        trackedLabelNames: selectedLabelNames,
        scheduleEnabled: true,
        scheduleHour: scheduleTime.hour,
        scheduleMinute: scheduleTime.minute,
        scheduleTimezone,
        ingestFromDate: ingestFromDate || null,
        ingestToDate: ingestToDate || null,
        startFromNow,
        resetCursor
      })
    });

    state.ingestSetup.statusMessage = "Ingest setup saved.";
    const dateFilterPart =
      ingestFromDate || ingestToDate
        ? ` Date filter: ${ingestFromDate || "oldest"} to ${ingestToDate || "latest"}.`
        : "";
    setPipelineMessage(
      `Tracking ${selectedLabelIds.length} labels. Daily ingest at ${formatScheduleTime(
        scheduleTime.hour,
        scheduleTime.minute
      )} (${scheduleTimezone}). Max ${maxResults} messages/run.${dateFilterPart}`
    );
    closeIngestSetupModal();
    await refreshAuthState();
  } catch (error) {
    state.ingestSetup.statusMessage = `Save failed: ${error.message}`;
    renderIngestSetupModal();
  } finally {
    state.ingestSetup.saving = false;
    renderIngestSetupModal();
  }
}

async function resetPipelineDataForCurrentUser() {
  if (!state.auth.token) {
    setPipelineMessage("Sign in with Google before resetting pipeline data.");
    return;
  }

  const confirmed = window.confirm(
    "This will delete your archived emails, extraction runs, and extracted reports, and reset ingest cursor to oldest. Continue?"
  );
  if (!confirmed) {
    return;
  }

  state.ingestSetup.saving = true;
  state.ingestSetup.statusMessage = "Resetting ingest + extraction data...";
  renderIngestSetupModal();

  try {
    const payload = await apiFetch("/api/pipeline/reset", {
      method: "POST",
      body: JSON.stringify({
        clearArchives: true,
        clearExtraction: true,
        resetCursor: true
      })
    });

    state.archives.items = [];
    state.archives.total = 0;
    state.archives.fetchedAt = null;
    state.extraction.runs = [];
    state.extraction.runsTotal = 0;
    state.extraction.lastRun = null;
    state.extraction.reports = [];
    state.extraction.reportsTotal = 0;
    state.extraction.fetchedAt = null;
    state.extraction.statusMessage = "Extraction workspace reset. Run ingest with a small date range to retest.";
    state.digestApi.preview = null;
    state.digestApi.error = "";
    state.digestApi.lastSentAt = null;

    await Promise.allSettled([fetchArchives(), refreshExtractionWorkspace()]);

    setPipelineMessage(
      `Reset complete: removed ${payload.removedArchives || 0} archives, ${payload.removedExtractedReports || 0} extracted reports, ${payload.removedExtractionRuns || 0} extraction runs.`
    );
    state.ingestSetup.statusMessage = "Reset complete. Configure labels/date range, then run ingest.";
    renderAllDataViews();
  } catch (error) {
    const message = error instanceof Error ? error.message : "Reset failed";
    state.ingestSetup.statusMessage = `Reset failed: ${message}`;
    setPipelineMessage(`Reset failed: ${message}`);
  } finally {
    state.ingestSetup.saving = false;
    renderIngestSetupModal();
  }
}

async function refreshAuthState() {
  renderAuthUi();
  if (!state.auth.token) {
    state.auth.user = null;
    state.auth.gmailConnected = false;
    state.auth.ingestionPreferences = null;
    state.digestApi.preview = null;
    state.digestApi.error = "";
    state.digestApi.lastSentAt = null;
    state.archives.items = [];
    state.archives.total = 0;
    resetExtractionStateForSignedOut();
    renderAllDataViews();
    return;
  }

  state.auth.loading = true;
  renderAuthUi();

  try {
    const me = await apiFetch("/api/auth/me");
    state.auth.user = me.user;
    state.auth.gmailConnected = Boolean(me.gmail?.connected);
    state.auth.ingestionPreferences = me.ingestionPreferences || null;
    const trackedCount = Number(state.auth.ingestionPreferences?.trackedLabelIds?.length || 0);
    if (trackedCount > 0) {
      const scheduleHour = state.auth.ingestionPreferences?.scheduleHour ?? 7;
      const scheduleMinute = state.auth.ingestionPreferences?.scheduleMinute ?? 30;
      const scheduleZone = state.auth.ingestionPreferences?.scheduleTimezone || "Asia/Kolkata";
      setPipelineMessage(
        `Auth connected. Tracking ${trackedCount} labels. Daily sync at ${formatScheduleTime(
          scheduleHour,
          scheduleMinute
        )} (${scheduleZone}).`
      );
    } else if (me.ingestionPreferences?.query) {
      setPipelineMessage(`Auth connected. Gmail query default: ${me.ingestionPreferences.query}`);
    } else {
      setPipelineMessage("Auth connected.");
    }

    await fetchArchives();
    await refreshExtractionWorkspace();
    if (state.view === "digest") {
      await fetchDigestPreviewFromApi();
    }
  } catch {
    clearAuthToken();
    state.auth.user = null;
    state.auth.gmailConnected = false;
    state.auth.ingestionPreferences = null;
    state.digestApi.preview = null;
    state.digestApi.error = "";
    state.digestApi.lastSentAt = null;
    resetExtractionStateForSignedOut();
    setPipelineMessage("Session expired. Please sign in with Google again.");
  } finally {
    state.auth.loading = false;
    renderAuthUi();
    renderAllDataViews();
  }
}

async function startGoogleAuth() {
  try {
    const redirectUri = `${window.location.origin}${window.location.pathname}`;
    const response = await fetch(
      `${getApiBase()}/api/auth/google/url?redirect_uri=${encodeURIComponent(redirectUri)}`
    );

    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Unable to start Google auth flow");
    }

    window.location.assign(payload.authUrl);
  } catch (error) {
    setPipelineMessage(`Google auth start failed: ${error.message}`);
  }
}

async function signOut() {
  try {
    if (state.auth.token) {
      await apiFetch("/api/auth/logout", { method: "POST" });
    }
  } catch {
    // Continue clearing local session regardless of API result.
  }

  clearAuthToken();
  state.auth.user = null;
  state.auth.gmailConnected = false;
  state.auth.ingestionPreferences = null;
  state.digestApi.preview = null;
  state.digestApi.error = "";
  state.digestApi.lastSentAt = null;
  state.archives.items = [];
  state.archives.total = 0;
  resetExtractionStateForSignedOut();
  closeIngestSetupModal();
  setPipelineMessage("Signed out.");
  renderAll();
}

function handleAuthCallbackFromHash() {
  const hash = window.location.hash.replace(/^#/, "");
  if (!hash) {
    return;
  }

  const params = new URLSearchParams(hash);
  const authState = params.get("auth");

  if (!authState) {
    return;
  }

  if (authState === "success") {
    const token = params.get("token") || "";
    const email = params.get("email") || "";
    if (token) {
      state.auth.token = token;
      localStorage.setItem(STORAGE_KEYS.authToken, token);
      setPipelineMessage(`Google auth successful for ${email || "connected user"}.`);
    }
  }

  if (authState === "error") {
    const message = params.get("message") || "Google auth failed";
    setPipelineMessage(`Google auth error: ${message}`);
  }

  history.replaceState(null, "", `${window.location.pathname}${window.location.search}`);
}

async function fetchArchives() {
  if (!state.auth.token) {
    state.archives.items = [];
    state.archives.total = 0;
    return;
  }

  try {
    const response = await apiFetch("/api/email-archives?limit=100&offset=0");
    state.archives.items = Array.isArray(response.items) ? response.items : [];
    state.archives.total = Number(response.total ?? state.archives.items.length);
    state.archives.fetchedAt = new Date().toISOString();
    hydrateBrokerFilter();
    hydrateCompanySelect();
  } catch (error) {
    setPipelineMessage(`Failed to load archives: ${error.message}`);
  }
}

function resetExtractionStateForSignedOut() {
  state.extraction.statusMessage = "Sign in with Google to access extraction runs and extracted reports.";
  state.extraction.runStarting = false;
  state.extraction.abortingRunId = "";
  state.extraction.runsLoading = false;
  state.extraction.reportsLoading = false;
  state.extraction.runsError = "";
  state.extraction.reportsError = "";
  state.extraction.runs = [];
  state.extraction.runsTotal = 0;
  state.extraction.lastRun = null;
  state.extraction.reports = [];
  state.extraction.reportsTotal = 0;
  state.extraction.fetchedAt = null;
}

async function refreshExtractionWorkspace() {
  await Promise.allSettled([fetchExtractionRuns({ silent: true }), fetchExtractedReports({ silent: true })]);

  if (
    state.extraction.endpointStatus.trigger === "unknown" &&
    state.extraction.endpointStatus.runs === "unavailable" &&
    state.extraction.endpointStatus.reports === "unavailable"
  ) {
    state.extraction.endpointStatus.trigger = "unavailable";
  }

  const unavailableRuns = state.extraction.endpointStatus.runs === "unavailable";
  const unavailableReports = state.extraction.endpointStatus.reports === "unavailable";
  const unavailableTrigger = state.extraction.endpointStatus.trigger === "unavailable";

  if (unavailableRuns && unavailableReports && unavailableTrigger) {
    state.extraction.statusMessage = "Extraction endpoints are unavailable in this backend. Workspace is in standby mode.";
  } else if (!state.extraction.runsError && !state.extraction.reportsError && state.extraction.fetchedAt) {
    state.extraction.statusMessage = `Extraction data refreshed at ${new Date(state.extraction.fetchedAt).toLocaleTimeString()}.`;
  }

  renderExtractionWorkspace();
}

async function triggerExtractionRun() {
  if (state.extraction.runStarting) {
    return;
  }

  if (!state.auth.token) {
    state.extraction.statusMessage = "Sign in with Google before running extraction.";
    renderExtractionWorkspace();
    return;
  }

  state.extraction.runStarting = true;
  state.extraction.statusMessage = "Triggering extraction run...";
  renderExtractionWorkspace();

  try {
    const result = await callExtractionEndpoint("trigger", {
      method: "POST",
      body: JSON.stringify({
        includeAlreadyExtracted: state.extraction.reprocessExisting === true,
        trigger: state.extraction.reprocessExisting ? "manual_api_reprocess" : "manual_api"
      })
    });
    if (result.ok) {
      const normalized = normalizeExtractionRunsPayload(result.payload);
      if (normalized.lastRun) {
        state.extraction.lastRun = normalized.lastRun;
      }
      state.extraction.statusMessage = "Extraction run triggered. Refreshing run status and reports...";
      setPipelineMessage("Extraction run triggered.");
      await refreshExtractionWorkspace();
      return;
    }

    if (result.unauthorized) {
      state.extraction.statusMessage = "Extraction run requires an authenticated session.";
      return;
    }

    if (result.unavailable) {
      state.extraction.statusMessage = "Extraction trigger endpoint unavailable in this deployment.";
      return;
    }

    state.extraction.statusMessage = `Extraction run failed: ${result.error?.message || "Unknown error"}`;
  } finally {
    state.extraction.runStarting = false;
    renderExtractionWorkspace();
  }
}

async function abortExtractionRun(runId) {
  const normalizedRunId = String(runId ?? "").trim();
  if (!normalizedRunId || !state.auth.token) {
    return;
  }

  const run = state.extraction.runs.find((entry) => entry.id === normalizedRunId);
  if (!run || !isExtractionRunAbortable(run)) {
    return;
  }

  const confirmed = window.confirm(`Abort extraction run ${normalizedRunId}?`);
  if (!confirmed) {
    return;
  }

  state.extraction.abortingRunId = normalizedRunId;
  state.extraction.statusMessage = `Sending abort request for run ${normalizedRunId}...`;
  renderExtractionWorkspace();

  try {
    const payload = await apiFetch(`/api/extraction/runs/${encodeURIComponent(normalizedRunId)}/abort`, {
      method: "POST",
      body: JSON.stringify({
        reason: "Aborted from web UI"
      })
    });

    if (payload?.accepted) {
      const mode = payload.immediate ? "aborted immediately" : "abort requested";
      setPipelineMessage(`Extraction run ${normalizedRunId}: ${mode}.`);
      state.extraction.statusMessage = `Extraction run ${normalizedRunId}: ${mode}.`;
    } else {
      setPipelineMessage(`Extraction run ${normalizedRunId} is already finished.`);
      state.extraction.statusMessage = `Extraction run ${normalizedRunId} is already finished.`;
    }

    await refreshExtractionWorkspace();
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown abort error";
    state.extraction.statusMessage = `Failed to abort run ${normalizedRunId}: ${message}`;
    setPipelineMessage(`Failed to abort extraction run: ${message}`);
  } finally {
    state.extraction.abortingRunId = "";
    renderExtractionWorkspace();
  }
}

async function fetchExtractionRuns(options = {}) {
  const silent = Boolean(options.silent);
  state.extraction.runsLoading = true;
  state.extraction.runsError = "";
  if (!silent && !state.extraction.runStarting) {
    state.extraction.statusMessage = "Loading extraction run history...";
  }
  renderExtractionWorkspace();

  try {
    const result = await callExtractionEndpoint("runs", {}, { limit: 20, offset: 0 });
    if (!result.ok) {
      if (result.unauthorized) {
        state.extraction.runs = [];
        state.extraction.runsTotal = 0;
        state.extraction.lastRun = null;
        if (!silent) {
          state.extraction.statusMessage = "Sign in to view extraction run history.";
        }
        return;
      }

      if (result.unavailable) {
        state.extraction.runs = [];
        state.extraction.runsTotal = 0;
        state.extraction.lastRun = null;
        if (!silent) {
          state.extraction.statusMessage = "Extraction run history endpoint unavailable.";
        }
        return;
      }

      throw result.error || new Error("Failed to fetch extraction runs");
    }

    const normalized = normalizeExtractionRunsPayload(result.payload);
    state.extraction.runs = normalized.items;
    state.extraction.runsTotal = normalized.total;
    state.extraction.lastRun = normalized.lastRun;
  } catch (error) {
    state.extraction.runsError = error instanceof Error ? error.message : "Unknown extraction runs error";
    if (!silent) {
      state.extraction.statusMessage = `Failed to load extraction runs: ${state.extraction.runsError}`;
    }
  } finally {
    state.extraction.runsLoading = false;
    renderExtractionWorkspace();
  }
}

async function fetchExtractedReports(options = {}) {
  const silent = Boolean(options.silent);
  state.extraction.reportsLoading = true;
  state.extraction.reportsError = "";
  if (!silent) {
    state.extraction.statusMessage = "Loading extracted reports...";
  }
  renderExtractionWorkspace();

  try {
    const result = await callExtractionEndpoint("reports", {}, {
      limit: 250,
      offset: 0,
      includeDuplicates: state.extraction.filters.includeDuplicates ? "true" : "false"
    });
    if (!result.ok) {
      if (result.unauthorized) {
        state.extraction.reports = [];
        state.extraction.reportsTotal = 0;
        if (!silent) {
          state.extraction.statusMessage = "Sign in to view extracted reports.";
        }
        return;
      }

      if (result.unavailable) {
        state.extraction.reports = [];
        state.extraction.reportsTotal = 0;
        if (!silent) {
          state.extraction.statusMessage = "Extracted reports endpoint unavailable.";
        }
        return;
      }

      throw result.error || new Error("Failed to fetch extracted reports");
    }

    const normalized = normalizeExtractedReportsPayload(result.payload);
    state.extraction.reports = normalized.items;
    state.extraction.reportsTotal = normalized.total;
    state.extraction.fetchedAt = new Date().toISOString();
  } catch (error) {
    state.extraction.reportsError = error instanceof Error ? error.message : "Unknown extracted reports error";
    if (!silent) {
      state.extraction.statusMessage = `Failed to load extracted reports: ${state.extraction.reportsError}`;
    }
  } finally {
    state.extraction.reportsLoading = false;
    renderExtractionWorkspace();
  }
}

async function callExtractionEndpoint(kind, requestOptions = {}, query = null) {
  const existing = state.extraction.endpoints[kind];
  const fallbackCandidates = Array.isArray(extractionEndpointCatalog[kind]) ? extractionEndpointCatalog[kind] : [];
  const candidates = existing
    ? [existing, ...fallbackCandidates.filter((candidate) => candidate !== existing)]
    : [...fallbackCandidates];
  let lastMissingError = null;

  for (const endpointBase of candidates) {
    const endpoint = buildEndpointWithQuery(endpointBase, query);
    try {
      const payload = await apiFetch(endpoint, requestOptions);
      state.extraction.endpoints[kind] = endpointBase;
      state.extraction.endpointStatus[kind] = "ready";
      return { ok: true, payload, endpoint: endpointBase };
    } catch (error) {
      const status = Number(error?.status || 0);

      if (status === 401 || status === 403) {
        state.extraction.endpoints[kind] = endpointBase;
        state.extraction.endpointStatus[kind] = "auth_required";
        return { ok: false, unauthorized: true, error, endpoint: endpointBase };
      }

      if (status === 404 || status === 405 || status === 501) {
        lastMissingError = error;
        continue;
      }

      state.extraction.endpoints[kind] = endpointBase;
      state.extraction.endpointStatus[kind] = "error";
      return { ok: false, failed: true, error, endpoint: endpointBase };
    }
  }

  state.extraction.endpoints[kind] = "";
  state.extraction.endpointStatus[kind] = "unavailable";
  return {
    ok: false,
    unavailable: true,
    error: lastMissingError || new Error("Endpoint unavailable")
  };
}

function buildEndpointWithQuery(endpoint, query) {
  if (!query || typeof query !== "object") {
    return endpoint;
  }

  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === null || value === undefined || value === "") {
      continue;
    }
    params.set(key, String(value));
  }

  const queryString = params.toString();
  return queryString ? `${endpoint}?${queryString}` : endpoint;
}

function normalizeExtractionRunsPayload(payload) {
  const items = extractItemsFromPayload(payload, ["items", "runs", "results"]).map((entry) => normalizeExtractionRun(entry)).filter(Boolean);
  const sortedItems = items.sort((left, right) => right.startedAtEpoch - left.startedAtEpoch);
  const totalValue = Number(
    payload?.total ??
      payload?.count ??
      payload?.runsTotal ??
      payload?.meta?.total ??
      payload?.data?.total ??
      sortedItems.length
  );
  const total = Number.isFinite(totalValue) ? Math.max(0, Math.floor(totalValue)) : sortedItems.length;
  const lastRunFromPayload =
    normalizeExtractionRun(payload?.lastRun) ||
    normalizeExtractionRun(payload?.latestRun) ||
    normalizeExtractionRun(payload?.latest) ||
    normalizeExtractionRun(payload?.data?.lastRun);
  const lastRun = lastRunFromPayload || sortedItems[0] || null;

  return {
    items: sortedItems,
    total,
    lastRun
  };
}

function normalizeExtractedReportsPayload(payload) {
  const items = extractItemsFromPayload(payload, ["items", "reports", "records", "results"])
    .map((entry) => normalizeExtractedReport(entry))
    .filter(Boolean);
  const sortedItems = items.sort((left, right) => right.publishedAtEpoch - left.publishedAtEpoch);
  const totalValue = Number(
    payload?.total ??
      payload?.count ??
      payload?.reportsTotal ??
      payload?.meta?.total ??
      payload?.data?.total ??
      sortedItems.length
  );
  const total = Number.isFinite(totalValue) ? Math.max(0, Math.floor(totalValue)) : sortedItems.length;

  return {
    items: sortedItems,
    total
  };
}

function extractItemsFromPayload(payload, keys = []) {
  if (Array.isArray(payload)) {
    return payload;
  }

  if (!payload || typeof payload !== "object") {
    return [];
  }

  for (const key of keys) {
    if (Array.isArray(payload[key])) {
      return payload[key];
    }
  }

  if (payload.data && typeof payload.data === "object") {
    if (Array.isArray(payload.data)) {
      return payload.data;
    }

    for (const key of keys) {
      if (Array.isArray(payload.data[key])) {
        return payload.data[key];
      }
    }

    if (Array.isArray(payload.data.items)) {
      return payload.data.items;
    }
  }

  return [];
}

function normalizeExtractionRun(entry) {
  if (!entry || typeof entry !== "object") {
    return null;
  }

  const stats = entry.stats && typeof entry.stats === "object" ? entry.stats : {};
  const startedAt = normalizeDateTimeValue(
    pickFirstValue(entry, [
      "startedAt",
      "started_at",
      "runStartedAt",
      "run_started_at",
      "createdAt",
      "created_at",
      "queuedAt",
      "queued_at",
      "timestamp"
    ])
  );
  const status = String(pickFirstValue(entry, ["status", "state", "runStatus", "run_status"], "unknown") || "unknown").trim();
  const abortRequestedAt = normalizeDateTimeValue(pickFirstValue(entry, ["abortRequestedAt", "abort_requested_at"], ""));
  const abortedAt = normalizeDateTimeValue(pickFirstValue(entry, ["abortedAt", "aborted_at"], ""));
  const abortPending =
    status.toLowerCase() === "running" &&
    Boolean(abortRequestedAt) &&
    !abortedAt &&
    pickFirstValue(entry, ["abortPending", "abort_pending"], true) !== false;
  const processedCount = pickFirstNumber(entry, [
    "processedCount",
    "processed_count",
    "archivesProcessed",
    "archives_processed",
    "totalProcessed",
    "total_processed",
    "sourceCount",
    "source_count"
  ], pickFirstNumber(stats, ["processedArchives", "processed_archives", "archivesProcessed", "archives_processed"], 0));
  const extractedCount = pickFirstNumber(entry, [
    "extractedCount",
    "extracted_count",
    "reportsExtracted",
    "reports_extracted",
    "insertedCount",
    "inserted_count",
    "createdCount",
    "created_count"
  ], pickFirstNumber(stats, ["extractedReports", "extracted_reports", "reportsExtracted", "reports_extracted"], 0));
  const duplicatesCount = pickFirstNumber(entry, [
    "duplicatesCount",
    "duplicates_count",
    "duplicateCount",
    "duplicate_count",
    "dedupedCount",
    "deduped_count"
  ], pickFirstNumber(stats, ["duplicateReports", "duplicate_reports", "duplicates", "deduped"], 0));

  return {
    id: String(pickFirstValue(entry, ["id", "runId", "run_id", "jobId", "job_id"], "") || "").trim(),
    startedAt,
    status: status || "unknown",
    abortRequestedAt,
    abortedAt,
    abortPending,
    processedCount,
    extractedCount,
    duplicatesCount,
    startedAtEpoch: parseDateValue(startedAt)?.getTime() || 0
  };
}

function normalizeExtractedReport(entry) {
  if (!entry || typeof entry !== "object") {
    return null;
  }

  const companyCanonical = String(
    pickFirstValue(entry, ["companyCanonical", "company_canonical", "company", "companyName", "company_name"], "")
  ).trim();
  const companyRaw = String(pickFirstValue(entry, ["companyRaw", "company_raw", "companyOriginal", "company_original"], "")).trim();
  const keyPointsValue = pickFirstValue(entry, ["keyPoints", "key_points", "points"], []);
  const keyPoints = Array.isArray(keyPointsValue)
    ? keyPointsValue.map((value) => truncateDisplayText(String(value).trim(), 180)).filter(Boolean)
    : [];
  const summary = truncateDisplayText(String(pickFirstValue(entry, ["summary", "snippet", "abstract"], "")).trim(), 420);
  const publishedAt = normalizeDateTimeValue(
    pickFirstValue(
      entry,
      ["publishedAt", "published_at", "reportDate", "report_date", "createdAt", "created_at", "updatedAt", "updated_at"],
      ""
    )
  );
  const sourceUrlRaw = String(pickFirstValue(entry, ["sourceUrl", "source_url", "archiveUrl", "archive_url", "url"], "")).trim();

  return {
    archiveId: String(pickFirstValue(entry, ["archiveId", "archive_id", "emailArchiveId", "email_archive_id"], "")).trim(),
    userId: String(pickFirstValue(entry, ["userId", "user_id"], "")).trim(),
    broker: String(pickFirstValue(entry, ["broker", "brokerName", "broker_name"], "Unmapped Broker")).trim() || "Unmapped Broker",
    companyCanonical: companyCanonical || companyRaw || "Unknown Company",
    companyRaw,
    reportType: String(pickFirstValue(entry, ["reportType", "report_type", "type", "category"], "Unknown")).trim() || "Unknown",
    title: String(pickFirstValue(entry, ["title", "headline", "subject"], "(Untitled report)")).trim() || "(Untitled report)",
    summary: summary || keyPoints.join(" | ") || "(No summary)",
    keyPoints,
    publishedAt,
    confidence: normalizeConfidence(pickFirstValue(entry, ["confidence", "score", "probability"], null)),
    duplicateKey: String(pickFirstValue(entry, ["duplicateKey", "duplicate_key", "dedupeKey", "dedupe_key"], "")).trim(),
    duplicateOfReportId: String(
      pickFirstValue(entry, ["duplicateOfReportId", "duplicate_of_report_id", "duplicateOfId", "duplicate_of_id"], "")
    ).trim(),
    dedupeMethod: String(pickFirstValue(entry, ["dedupeMethod", "dedupe_method"], "")).trim(),
    sourceUrl: /^https?:\/\//i.test(sourceUrlRaw) ? sourceUrlRaw : "",
    publishedAtEpoch: parseDateValue(publishedAt)?.getTime() || 0
  };
}

function truncateDisplayText(value, maxLength = 420) {
  const text = String(value ?? "").trim();
  if (!text || text.length <= maxLength) {
    return text;
  }

  const limit = Math.max(1, maxLength - 3);
  const truncated = text.slice(0, limit);
  const breakAt = truncated.lastIndexOf(" ");
  if (breakAt > Math.floor(limit * 0.6)) {
    return `${truncated.slice(0, breakAt)}...`;
  }

  return `${truncated}...`;
}

function pickFirstValue(entry, keys, fallback = "") {
  if (!entry || typeof entry !== "object") {
    return fallback;
  }

  for (const key of keys) {
    if (!Object.prototype.hasOwnProperty.call(entry, key)) {
      continue;
    }
    const value = entry[key];
    if (value === null || value === undefined) {
      continue;
    }
    if (typeof value === "string" && value.trim() === "") {
      continue;
    }
    return value;
  }

  return fallback;
}

function pickFirstNumber(entry, keys, fallback = 0) {
  for (const key of keys) {
    if (!Object.prototype.hasOwnProperty.call(entry, key)) {
      continue;
    }
    const value = Number(entry[key]);
    if (Number.isFinite(value)) {
      return value;
    }
  }

  return fallback;
}

function normalizeConfidence(value) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return null;
  }

  if (numericValue >= 0 && numericValue <= 1) {
    return numericValue * 100;
  }

  return Math.max(0, Math.min(100, numericValue));
}

function normalizeDateTimeValue(value) {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return "";
  }

  const parsed = parseDateValue(raw);
  if (!parsed || Number.isNaN(parsed.getTime())) {
    return raw;
  }

  return parsed.toISOString();
}

function parseDateValue(value) {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return null;
  }

  const numeric = Number(raw);
  if (Number.isFinite(numeric) && numeric > 0) {
    const millis = numeric > 10_000_000_000 ? numeric : numeric * 1000;
    const numericDate = new Date(millis);
    if (!Number.isNaN(numericDate.getTime())) {
      return numericDate;
    }
  }

  const parsedMillis = Date.parse(raw);
  if (Number.isFinite(parsedMillis)) {
    return new Date(parsedMillis);
  }

  return null;
}

function extractExchangeSync(payload, exchange) {
  const normalizedExchange =
    exchange === "NSE+BSE" || exchange === "BSE+NSE" || exchange === "COMBINED"
      ? "COMBINED"
      : exchange === "DEDUP"
        ? "DEDUP"
        : exchange;
  const fallbackAtKey =
    normalizedExchange === "BSE"
      ? "lastBseSyncAt"
      : normalizedExchange === "COMBINED"
        ? "lastCombinedSyncAt"
        : normalizedExchange === "DEDUP"
          ? "lastDedupSyncAt"
          : "lastNseSyncAt";
  const fallbackStatsKey =
    normalizedExchange === "BSE"
      ? "lastBseSyncStats"
      : normalizedExchange === "COMBINED"
        ? "lastCombinedSyncStats"
        : normalizedExchange === "DEDUP"
          ? "lastDedupSyncStats"
          : "lastNseSyncStats";
  let syncAt = null;
  let syncStats = null;

  if (typeof payload?.lastSyncAt === "string") {
    syncAt = payload.lastSyncAt;
  } else if (payload?.lastSyncAt && typeof payload.lastSyncAt === "object") {
    const objectSyncAt = payload.lastSyncAt;
    if (typeof objectSyncAt[exchange] === "string") {
      syncAt = objectSyncAt[exchange];
    } else if (typeof objectSyncAt[normalizedExchange] === "string") {
      syncAt = objectSyncAt[normalizedExchange];
    }
  } else if (typeof payload?.[fallbackAtKey] === "string") {
    syncAt = payload[fallbackAtKey];
  }

  if (payload?.lastSyncStats && typeof payload.lastSyncStats === "object" && !Array.isArray(payload.lastSyncStats)) {
    if (payload.lastSyncStats.exchange === exchange || payload.lastSyncStats.exchange === normalizedExchange) {
      syncStats = payload.lastSyncStats;
    } else if (payload.lastSyncStats[exchange] && typeof payload.lastSyncStats[exchange] === "object") {
      syncStats = payload.lastSyncStats[exchange];
    } else if (payload.lastSyncStats[normalizedExchange] && typeof payload.lastSyncStats[normalizedExchange] === "object") {
      syncStats = payload.lastSyncStats[normalizedExchange];
    }
  }

  if (!syncStats && payload?.[fallbackStatsKey] && typeof payload[fallbackStatsKey] === "object") {
    syncStats = payload[fallbackStatsKey];
  }

  return { syncAt, syncStats };
}

function fallbackSymbolMatch(item, exchange, symbol) {
  if (!symbol) {
    return true;
  }

  const query = symbol.toUpperCase();
  if (exchange === "NSE+BSE" || exchange === "BSE+NSE" || exchange === "COMBINED" || exchange === "DEDUP") {
    const combinedSymbol = String(item?.symbol ?? "").toUpperCase();
    const combinedCompany = String(item?.company ?? "").toUpperCase();
    const combinedIsin = String(item?.isin ?? "").toUpperCase();
    if (combinedSymbol === query || combinedCompany.includes(query) || combinedIsin === query) {
      return true;
    }

    const sourceAnnouncements = Array.isArray(item?.sourceAnnouncements) ? item.sourceAnnouncements : [];
    return sourceAnnouncements.some((source) => {
      const sourceSymbol = String(source?.symbol ?? "").toUpperCase();
      const sourceCompany = String(source?.company ?? "").toUpperCase();
      const sourceIsin = String(source?.isin ?? "").toUpperCase();
      return sourceSymbol === query || sourceCompany.includes(query) || sourceIsin === query;
    });
  }

  if (exchange === "BSE") {
    const scripCode = String(item?.scrip_code ?? "").toUpperCase();
    const companyName = String(item?.company_name ?? "").toUpperCase();
    const isin = String(item?.isin ?? "").toUpperCase();
    return scripCode === query || companyName.includes(query) || isin === query;
  }

  const nseSymbol = String(item?.symbol ?? "").toUpperCase();
  const companyName = String(item?.sm_name ?? "").toUpperCase();
  const isin = String(item?.sm_isin ?? "").toUpperCase();
  return nseSymbol === query || companyName.includes(query) || isin === query;
}

function normalizeAiLabelFilterValue(value) {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();
  if (!normalized || normalized === "all") {
    return "all";
  }
  return normalized;
}

function matchesNotificationAiLabelFilter(item, aiLabelFilter) {
  const filter = normalizeAiLabelFilterValue(aiLabelFilter);
  if (filter === "all") {
    return true;
  }

  const aiStatus = String(item?.ai_status ?? "")
    .trim()
    .toLowerCase();
  const aiLabel = String(item?.ai_label ?? "")
    .trim()
    .toLowerCase();
  const reviewLabel = String(item?.review_label ?? "")
    .trim()
    .toLowerCase();

  if (filter === "pending") {
    return aiStatus === "missing" || !aiLabel;
  }

  if (filter === "failed") {
    return aiStatus === "failed";
  }

  if (filter === "reviewed") {
    return Boolean(reviewLabel);
  }

  return aiLabel === filter || reviewLabel === filter;
}

function setNotificationSuggestionContext(item) {
  const dedupAnnouncementKey = getDedupAnnouncementKey(item);
  if (!dedupAnnouncementKey) {
    return;
  }

  state.notifications.suggestionContext = {
    dedupAnnouncementKey,
    attachmentUrl: String(item?.attachment_url ?? "").trim(),
    existingAiLabel: String(item?.ai_label ?? "").trim(),
    existingReviewLabel: String(item?.review_label ?? "").trim(),
    symbol: String(item?.symbol ?? "").trim(),
    company: String(item?.company ?? "").trim()
  };
}

function clearNotificationSuggestionContext() {
  state.notifications.suggestionContext = null;
}

function getNotificationAiCategories() {
  if (Array.isArray(state.notifications.aiCategories) && state.notifications.aiCategories.length > 0) {
    return Array.from(
      new Set(
        state.notifications.aiCategories
          .map((value) => String(value ?? "").trim().toLowerCase())
          .filter((value) => Boolean(value))
      )
    ).sort();
  }
  return [...fallbackAiCategories];
}

function formatAiCategoryLabel(value) {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    return "-";
  }

  return normalized
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function getDedupAnnouncementKey(item) {
  const dedupAnnouncementKey = String(item?.dedupAnnouncementKey ?? item?.mergedAnnouncementKey ?? item?.announcementKey ?? "").trim();
  return dedupAnnouncementKey || "";
}

function getNotificationsApiBase() {
  return notificationsApiBaseFallback.replace(/\/$/, "");
}

async function fetchNotificationCategories() {
  const normalizePayload = (payload) =>
    Array.isArray(payload?.categories)
      ? payload.categories
          .map((item) => String(item?.value ?? item ?? "").trim())
          .filter((value) => Boolean(value))
      : [];

  const notificationsBase = getNotificationsApiBase();
  try {
    const payload = await apiFetchFromBase("/api/ai/categories", notificationsBase, {});
    const categories = normalizePayload(payload);
    state.notifications.aiCategories = categories.length > 0 ? categories : [...fallbackAiCategories];
    return;
  } catch {
    // Fall through to current base or static categories.
  }

  const currentBase = getApiBase();
  if (currentBase !== notificationsBase) {
    try {
      const payload = await apiFetch("/api/ai/categories");
      const categories = normalizePayload(payload);
      state.notifications.aiCategories = categories.length > 0 ? categories : [...fallbackAiCategories];
      return;
    } catch {
      // Fall through to static categories.
    }
  }

  state.notifications.aiCategories = [...fallbackAiCategories];
}

function normalizeSuggestionCategoryInput(value) {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();
  if (!normalized) {
    return "";
  }
  return normalized.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 120);
}

function upsertNotificationSuggestionRecord(record) {
  const id = String(record?.id ?? "").trim();
  if (!id) {
    return;
  }

  const existingIndex = state.notifications.suggestions.findIndex((item) => String(item?.id ?? "").trim() === id);
  if (existingIndex >= 0) {
    state.notifications.suggestions[existingIndex] = record;
  } else {
    state.notifications.suggestions.unshift(record);
  }
}

async function fetchNotificationSuggestions(options = {}) {
  const force = Boolean(options?.force);
  if (state.notifications.suggestionsLoading) {
    return;
  }
  if (state.notifications.suggestionsLoaded && !force) {
    return;
  }

  state.notifications.suggestionsLoading = true;
  state.notifications.suggestionsError = "";
  renderNotifications();

  const endpoint = "/api/ai/suggestions?limit=20&status=ALL";
  let response = null;
  let requestError = null;

  const notificationsBase = getNotificationsApiBase();

  try {
    response = await apiFetchFromBase(endpoint, notificationsBase, {});
  } catch (error) {
    requestError = error instanceof Error ? error : new Error("Unknown suggestion fetch error");
  }

  if (!response) {
    const currentBase = getApiBase();
    if (currentBase !== notificationsBase) {
      try {
        response = await apiFetch(endpoint);
      } catch (error) {
        const fallbackError = error instanceof Error ? error.message : "Unknown suggestion fallback error";
        const primary = requestError ? requestError.message : "";
        requestError = new Error(primary ? `${primary}; ${fallbackError}` : fallbackError);
      }
    }
  }

  if (response) {
    state.notifications.suggestions = Array.isArray(response?.suggestions) ? response.suggestions : [];
    state.notifications.suggestionsTotal = Number(response?.total ?? state.notifications.suggestions.length);
    state.notifications.suggestionsLoaded = true;
    state.notifications.suggestionsError = "";
  } else if (requestError) {
    state.notifications.suggestionsError = requestError.message;
  }

  state.notifications.suggestionsLoading = false;
  renderNotifications();
}

async function submitNotificationSuggestion() {
  const rawCategory = refs.notificationsSuggestionCategory?.value ?? "";
  const comment = String(refs.notificationsSuggestionComment?.value ?? "")
    .trim()
    .slice(0, 3000);
  const suggestedCategory = normalizeSuggestionCategoryInput(rawCategory);

  if (!suggestedCategory) {
    state.notifications.suggestionFeedback = "Enter a suggested category.";
    renderNotifications();
    return;
  }

  if (!comment) {
    state.notifications.suggestionFeedback = "Enter a comment for this suggestion.";
    renderNotifications();
    return;
  }

  const context = state.notifications.suggestionContext || null;
  const payload = {
    suggestedCategory,
    comment,
    source: "frontend",
    suggestedBy: "user",
    exampleDedupAnnouncementKey: context?.dedupAnnouncementKey || "",
    exampleAttachmentUrl: context?.attachmentUrl || "",
    existingAiLabel: context?.existingAiLabel || "",
    existingReviewLabel: context?.existingReviewLabel || ""
  };

  state.notifications.suggestionSaving = true;
  state.notifications.suggestionFeedback = "Submitting suggestion...";
  renderNotifications();

  let response = null;
  let requestError = null;
  const notificationsBase = getNotificationsApiBase();

  try {
    response = await apiFetchFromBase(
      "/api/ai/suggestions",
      notificationsBase,
      {
        method: "POST",
        body: JSON.stringify(payload)
      },
      false
    );
  } catch (error) {
    requestError = error instanceof Error ? error : new Error("Unknown suggestion save error");
  }

  if (!response) {
    const currentBase = getApiBase();
    if (currentBase !== notificationsBase) {
      try {
        response = await apiFetch("/api/ai/suggestions", {
          method: "POST",
          body: JSON.stringify(payload)
        });
      } catch (error) {
        const fallbackError = error instanceof Error ? error.message : "Unknown suggestion fallback error";
        const primary = requestError ? requestError.message : "";
        requestError = new Error(primary ? `${primary}; ${fallbackError}` : fallbackError);
      }
    }
  }

  if (!response) {
    state.notifications.suggestionSaving = false;
    state.notifications.suggestionFeedback = `Failed to submit suggestion: ${requestError?.message || "Unknown error"}`;
    renderNotifications();
    return;
  }

  const savedRecord = response?.suggestion && typeof response.suggestion === "object" ? response.suggestion : null;
  if (savedRecord) {
    upsertNotificationSuggestionRecord(savedRecord);
  }

  if (!state.notifications.aiCategories.includes(suggestedCategory)) {
    state.notifications.aiCategories = [...state.notifications.aiCategories, suggestedCategory].sort();
  }

  state.notifications.suggestionsTotal = Number(response?.total ?? state.notifications.suggestions.length);
  state.notifications.suggestionsLoaded = true;
  state.notifications.suggestionsError = "";
  state.notifications.suggestionSaving = false;
  const savedId = String(savedRecord?.id ?? "").trim();
  state.notifications.suggestionFeedback = savedId ? `Suggestion saved (ID: ${savedId}).` : "Suggestion saved.";
  if (refs.notificationsSuggestionComment) {
    refs.notificationsSuggestionComment.value = "";
  }
  renderNotifications();
}

async function saveNotificationReview(dedupAnnouncementKey, reviewedLabel) {
  const normalizedKey = String(dedupAnnouncementKey ?? "").trim();
  const normalizedLabel = String(reviewedLabel ?? "").trim().toLowerCase();
  if (!normalizedKey || !normalizedLabel) {
    state.notifications.reviewFeedback = "Missing key or label for review save.";
    renderNotifications();
    return;
  }

  state.notifications.reviewSaving = true;
  state.notifications.reviewSavingKey = normalizedKey;
  state.notifications.reviewFeedback = "Saving correct classification...";
  renderNotifications();

  try {
    const requestBody = JSON.stringify({
      reviewer: "web-ui",
      reviews: [
        {
          dedupAnnouncementKey: normalizedKey,
          reviewedLabel: normalizedLabel
        }
      ]
    });
    let response = null;
    let requestError = null;
    const notificationsBase = getNotificationsApiBase();

    try {
      response = await apiFetchFromBase(
        "/api/ai/reviews/bulk",
        notificationsBase,
        {
          method: "POST",
          body: requestBody
        },
        false
      );
    } catch (error) {
      requestError = error instanceof Error ? error : new Error("Unknown review save error");
    }

    if (!response) {
      const currentBase = getApiBase();
      if (currentBase !== notificationsBase) {
        response = await apiFetch("/api/ai/reviews/bulk", {
          method: "POST",
          body: requestBody
        });
      } else if (requestError) {
        throw requestError;
      }
    }

    if (!response) {
      throw requestError || new Error("Failed to save reviewed label");
    }

    const updatedAt = String(response?.updatedAt ?? new Date().toISOString());
    for (const item of state.notifications.items) {
      if (getDedupAnnouncementKey(item) !== normalizedKey) {
        continue;
      }
      item.review_label = normalizedLabel;
      item.reviewed_at = updatedAt;
    }

    const diagnostics = response?.diagnostics && typeof response.diagnostics === "object" ? response.diagnostics : null;
    const agreementRate = Number(diagnostics?.agreementRate);
    const agreementText = Number.isFinite(agreementRate) ? ` Agreement: ${Math.round(agreementRate * 100)}%.` : "";
    const learningRules = Array.isArray(response?.promptLearning?.learnedRules) ? response.promptLearning.learnedRules : [];
    const learningText = learningRules.length > 0 ? ` Prompt rules active: ${learningRules.length}.` : "";
    state.notifications.reviewFeedback = `Saved correct classification.${agreementText}${learningText}`;
  } catch (error) {
    state.notifications.reviewFeedback = `Failed to save correct classification: ${error.message}`;
  } finally {
    state.notifications.reviewSaving = false;
    state.notifications.reviewSavingKey = "";
    renderNotifications();
  }
}

async function fetchNotifications() {
  const exchangeInput = String(refs.notificationsExchangeSelect?.value ?? "NSE").trim().toUpperCase();
  const exchange =
    exchangeInput === "BSE"
      ? "BSE"
      : exchangeInput === "DEDUP"
        ? "DEDUP"
        : exchangeInput === "NSE+BSE" || exchangeInput === "BSE+NSE" || exchangeInput === "COMBINED"
          ? "NSE+BSE"
          : "NSE";
  if (exchange === "DEDUP" && state.notifications.aiCategories.length === 0) {
    await fetchNotificationCategories();
  }
  const aiLabelFilter =
    exchange === "DEDUP"
      ? normalizeAiLabelFilterValue(refs.notificationsAiLabelFilter?.value ?? state.notifications.aiLabelFilter ?? "all")
      : "all";
  const symbol = (refs.notificationsSymbolInput?.value ?? "").trim().toUpperCase();
  const limitInput = Number.parseInt(refs.notificationsLimitSelect?.value ?? "50", 10);
  const limit = Number.isFinite(limitInput) ? Math.max(1, Math.min(500, limitInput)) : 50;
  const page = Math.max(1, Number.isFinite(state.notifications.page) ? Math.floor(state.notifications.page) : 1);
  const offset = (page - 1) * limit;

  notificationsRequestSeq += 1;
  const requestSeq = notificationsRequestSeq;
  if (notificationsAbortController) {
    notificationsAbortController.abort();
  }
  notificationsAbortController = new AbortController();
  const { signal } = notificationsAbortController;

  state.notifications.exchange = exchange;
  state.notifications.symbol = symbol;
  state.notifications.aiLabelFilter = aiLabelFilter;
  state.notifications.limit = limit;
  state.notifications.loading = true;
  state.notifications.error = "";
  renderNotifications();

  const params = new URLSearchParams({
    exchange,
    limit: String(limit),
    offset: String(offset)
  });

  if (symbol) {
    params.set("symbol", symbol);
  }
  if (exchange === "DEDUP" && aiLabelFilter !== "all") {
    params.set("aiLabel", aiLabelFilter);
  }

  const notificationsEndpoint = `/api/notifications/announcements?${params.toString()}`;
  const buildApiResult = (payload, sourceLabel) => {
    const items = Array.isArray(payload?.announcements) ? payload.announcements : [];
    const { syncAt, syncStats } = extractExchangeSync(payload, exchange);
    const total = Number(payload?.total ?? items.length);
    const payloadPage = Number(payload?.page ?? page);
    const totalPages = Number(payload?.totalPages ?? Math.max(1, Math.ceil(total / limit)));

    return {
      items,
      total,
      page: Number.isFinite(payloadPage) ? Math.max(1, Math.floor(payloadPage)) : page,
      totalPages: Number.isFinite(totalPages) ? Math.max(1, Math.floor(totalPages)) : Math.max(1, Math.ceil(total / limit)),
      lastSyncAt: syncAt,
      lastSyncStats: syncStats,
      source: sourceLabel
    };
  };

  let apiResult = null;
  let apiError = null;
  let fallbackResult = null;
  let fallbackError = null;

  try {
    const payload = await apiFetch(notificationsEndpoint, { signal });
    if (requestSeq !== notificationsRequestSeq) {
      return;
    }

    apiResult = buildApiResult(
      payload,
      exchange === "NSE+BSE"
        ? "backend API (NSE+BSE no dedup)"
        : exchange === "DEDUP"
          ? "backend API (Dedup: ISIN + PDF hash)"
          : `backend API (${exchange})`
    );
  } catch (error) {
    if (error?.name === "AbortError" || requestSeq !== notificationsRequestSeq) {
      return;
    }
    apiError = error instanceof Error ? error.message : "Unknown API error";
  }

  if (!apiResult) {
    const currentBase = getApiBase();
    const fallbackBase = notificationsApiBaseFallback.replace(/\/$/, "");

    if (currentBase !== fallbackBase) {
      try {
        const payload = await apiFetchFromBase(notificationsEndpoint, fallbackBase, { signal });
        if (requestSeq !== notificationsRequestSeq) {
          return;
        }

        apiResult = buildApiResult(
          payload,
          exchange === "NSE+BSE"
            ? "core API fallback (NSE+BSE no dedup)"
            : exchange === "DEDUP"
              ? "core API fallback (Dedup: ISIN + PDF hash)"
              : `core API fallback (${exchange})`
        );
      } catch (error) {
        if (error?.name === "AbortError" || requestSeq !== notificationsRequestSeq) {
          return;
        }
        const fallbackApiError = error instanceof Error ? error.message : "Unknown core API fallback error";
        apiError = apiError ? `${apiError}; ${fallbackApiError}` : fallbackApiError;
      }
    }
  }

  if (exchange !== "DEDUP") {
    try {
      const fallbackFile =
        exchange === "BSE"
          ? "./backend/data/bse_announcements.json"
          : exchange === "NSE+BSE"
            ? "./backend/data/combined_announcements.json"
            : "./backend/data/nse_announcements.json";
      const fallbackResponse = await fetch(fallbackFile, { cache: "no-store", signal });
      if (!fallbackResponse.ok) {
        throw new Error(`Fallback file not found (${fallbackResponse.status})`);
      }

      const fallbackPayload = await fallbackResponse.json();
      const allItems = Array.isArray(fallbackPayload?.announcements) ? fallbackPayload.announcements : [];
      const filteredItems = allItems.filter((item) => fallbackSymbolMatch(item, exchange, symbol));
      const normalizedItems = filteredItems.map((item) => ({
        ...item,
        exchange: item.exchange || exchange
      }));
      const { syncAt, syncStats } = extractExchangeSync(fallbackPayload, exchange);

      fallbackResult = {
        items: normalizedItems.slice(offset, offset + limit),
        total: normalizedItems.length,
        page,
        totalPages: Math.max(1, Math.ceil(normalizedItems.length / limit)),
        lastSyncAt: syncAt,
        lastSyncStats: syncStats,
        source:
          exchange === "NSE+BSE"
            ? "bundled NSE+BSE snapshot (no dedup)"
            : `bundled ${exchange} snapshot`
      };
    } catch (error) {
      if (error?.name === "AbortError" || requestSeq !== notificationsRequestSeq) {
        return;
      }
      fallbackError = error instanceof Error ? error.message : "Unknown fallback error";
    }
  }

  const apiLooksReady = apiResult && (apiResult.total > 0 || apiResult.lastSyncAt);
  const chosenResult = apiLooksReady ? apiResult : fallbackResult ?? apiResult;
  if (requestSeq !== notificationsRequestSeq) {
    return;
  }

  if (chosenResult) {
    const totalPages = Math.max(1, Number(chosenResult.totalPages ?? Math.ceil(chosenResult.total / limit)));
    const nextPage = Math.min(Math.max(Number(chosenResult.page ?? page), 1), totalPages);

    state.notifications.items = chosenResult.items;
    state.notifications.total = chosenResult.total;
    state.notifications.page = nextPage;
    state.notifications.totalPages = totalPages;
    state.notifications.lastSyncAt = chosenResult.lastSyncAt;
    state.notifications.lastSyncStats = chosenResult.lastSyncStats;
    state.notifications.source = chosenResult.source;
    state.notifications.error = "";

    if (nextPage !== page) {
      state.notifications.loading = false;
      renderNotifications();
      await fetchNotifications();
      return;
    }
  } else {
    state.notifications.items = [];
    state.notifications.total = 0;
    state.notifications.page = 1;
    state.notifications.totalPages = 1;
    state.notifications.lastSyncAt = null;
    state.notifications.lastSyncStats = null;
    state.notifications.source = "";
    state.notifications.error = apiError || fallbackError || "Unable to load announcements";
  }

  if (requestSeq !== notificationsRequestSeq) {
    return;
  }

  state.notifications.loading = false;
  renderNotifications();
}

function getVisibleArchives() {
  let records = [...state.archives.items];

  if (state.archives.brokerFilter !== "All") {
    records = records.filter((item) => item.broker === state.archives.brokerFilter);
  }

  if (state.archives.search) {
    records = records.filter((item) => {
      const haystack = `${item.subject || ""} ${item.from || ""} ${item.snippet || ""}`.toLowerCase();
      return haystack.includes(state.archives.search);
    });
  }

  return records;
}

function getArchiveTotalPages(totalRecords = null) {
  const total = totalRecords === null ? getVisibleArchives().length : Number(totalRecords);
  const pageSize = Math.max(1, Number.isFinite(state.archives.pageSize) ? Math.floor(state.archives.pageSize) : 20);
  return Math.max(1, Math.ceil(Math.max(0, total) / pageSize));
}

async function apiFetch(endpoint, options = {}) {
  return apiFetchFromBase(endpoint, getApiBase(), options, true);
}

async function apiFetchFromBase(endpoint, apiBase, options = {}, clearAuthOnUnauthorized = false) {
  const request = {
    method: options.method || "GET",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {})
    },
    signal: options.signal
  };

  if (options.body !== undefined) {
    request.body = options.body;
  }

  const normalizedBase = String(apiBase || "").replace(/\/$/, "");
  const shouldAttachAuth = Boolean(state.auth.token) && options.attachAuth !== false;
  if (shouldAttachAuth) {
    request.headers.Authorization = `Bearer ${state.auth.token}`;
  }

  const response = await fetch(`${normalizedBase}${endpoint}`, request);
  const payload = await response.json().catch(() => ({}));

  if (!response.ok) {
    if (clearAuthOnUnauthorized && response.status === 401) {
      clearAuthToken();
      renderAuthUi();
    }
    const requestError = new Error(payload.error || `Request failed (${response.status})`);
    requestError.status = response.status;
    requestError.payload = payload;
    throw requestError;
  }

  return payload;
}

function getApiBase() {
  return state.apiBase.replace(/\/$/, "");
}

function toApiAbsolute(relativeOrAbsoluteUrl) {
  if (!relativeOrAbsoluteUrl || relativeOrAbsoluteUrl === "#") {
    return "#";
  }

  if (/^https?:\/\//i.test(relativeOrAbsoluteUrl)) {
    return relativeOrAbsoluteUrl;
  }

  if (relativeOrAbsoluteUrl.startsWith("/")) {
    return `${getApiBase()}${relativeOrAbsoluteUrl}`;
  }

  return relativeOrAbsoluteUrl;
}

function addCompanyToList(kind, rawCompany) {
  const normalized = normalizeCompanyName(rawCompany);
  if (!normalized) {
    return;
  }

  if (kind === "priority") {
    if (!state.priorityCompanies.includes(normalized)) {
      state.priorityCompanies.push(normalized);
      persistList(STORAGE_KEYS.priority, state.priorityCompanies);
    }
  } else {
    if (!state.ignoredCompanies.includes(normalized)) {
      state.ignoredCompanies.push(normalized);
      persistList(STORAGE_KEYS.ignored, state.ignoredCompanies);
    }
  }

  renderAllDataViews();
}

function normalizeCompanyName(input) {
  const value = input.trim();
  if (!value) {
    return "";
  }

  const key = normalizeKey(value);
  for (const entry of state.dictionary) {
    if (normalizeKey(entry.canonical) === key) {
      return entry.canonical;
    }

    if (entry.aliases.some((alias) => normalizeKey(alias) === key)) {
      return entry.canonical;
    }
  }

  return value;
}

function renderRemovableChipList(values, extraClass, attrName) {
  if (values.length === 0) {
    return `<span class="chip">None</span>`;
  }

  return values
    .map(
      (value) =>
        `<span class="chip ${extraClass}">${escapeHtml(value)} <button class="remove" ${attrName}="${escapeAttribute(value)}" type="button">x</button></span>`
    )
    .join("");
}

function setPipelineMessage(message) {
  state.pipelineMessage = message;
  refs.pipelineStatus.textContent = `Pipeline status: ${message}`;
  renderDigest();
}

function clearAuthToken() {
  state.auth.token = "";
  localStorage.removeItem(STORAGE_KEYS.authToken);
}

async function copyToClipboard(text) {
  if (!navigator.clipboard || !window.isSecureContext) {
    return;
  }
  await navigator.clipboard.writeText(text);
}

function groupBy(items, field) {
  return items.reduce((acc, item) => {
    const key = item[field];
    if (!acc[key]) {
      acc[key] = [];
    }
    acc[key].push(item);
    return acc;
  }, {});
}

function debounce(callback, waitMs = 200) {
  let timeoutId = null;

  return (...args) => {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }

    timeoutId = setTimeout(() => {
      timeoutId = null;
      callback(...args);
    }, waitMs);
  };
}

function formatTime(value) {
  return new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatDateTime(value) {
  const parsed = parseDateValue(value);
  if (!parsed || Number.isNaN(parsed.getTime())) {
    const fallback = String(value ?? "").trim();
    return fallback || "-";
  }

  return parsed.toLocaleString([], {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function formatShortDate(value) {
  return new Date(value).toLocaleDateString([], { day: "2-digit", month: "short", year: "numeric" });
}

function formatCount(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "0";
  }

  return Math.max(0, Math.round(numeric)).toLocaleString();
}

function parseNotificationTimestamp(value) {
  const normalized = String(value ?? "").trim();
  if (!normalized || normalized === "-") {
    return null;
  }

  const parsedMillis = Date.parse(normalized);
  if (Number.isFinite(parsedMillis)) {
    return new Date(parsedMillis);
  }

  const compactDateTime = normalized.match(/^(\d{2})(\d{2})(\d{4})(\d{2})(\d{2})(\d{2})$/);
  if (compactDateTime) {
    const [, day, month, year, hour, minute, second] = compactDateTime;
    return new Date(Number(year), Number(month) - 1, Number(day), Number(hour), Number(minute), Number(second));
  }

  return null;
}

function formatNotificationTimestamp(value) {
  const parsed = parseNotificationTimestamp(value);
  if (!parsed || Number.isNaN(parsed.getTime())) {
    const fallback = String(value ?? "").trim();
    return fallback || "-";
  }

  const day = String(parsed.getDate()).padStart(2, "0");
  const month = parsed.toLocaleString("en-US", { month: "short" });
  const hour = String(parsed.getHours()).padStart(2, "0");
  const minute = String(parsed.getMinutes()).padStart(2, "0");
  return `${day}-${month} ${hour}:${minute}`;
}

function tagClassForType(type) {
  return `type-${type.toLowerCase().replace(/\s+/g, "-")}`;
}

function normalizeKey(value) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "").trim();
}

function loadList(key, fallback) {
  try {
    const value = localStorage.getItem(key);
    if (!value) {
      return [...fallback];
    }

    const parsed = JSON.parse(value);
    if (!Array.isArray(parsed)) {
      return [...fallback];
    }

    return parsed.filter((item) => typeof item === "string" && item.trim());
  } catch {
    return [...fallback];
  }
}

function persistList(key, value) {
  localStorage.setItem(key, JSON.stringify(value));
}

function loadString(key, fallback) {
  const value = localStorage.getItem(key);
  return value && value.trim() ? value : fallback;
}

function loadApiBase() {
  const normalizedDefault = defaultApiBase.replace(/\/$/, "");
  const stored = loadString(STORAGE_KEYS.apiBase, normalizedDefault).replace(/\/$/, "");
  if (!stored) {
    return normalizedDefault;
  }
  if (legacyApiBases.has(stored)) {
    localStorage.setItem(STORAGE_KEYS.apiBase, normalizedDefault);
    return normalizedDefault;
  }
  return stored;
}

function loadDictionary() {
  try {
    const value = localStorage.getItem(STORAGE_KEYS.dictionary);
    if (!value) {
      return [...defaultDictionary];
    }

    const parsed = JSON.parse(value);
    if (!Array.isArray(parsed)) {
      return [...defaultDictionary];
    }

    return parsed
      .filter((entry) => entry && typeof entry.canonical === "string" && typeof entry.ticker === "string")
      .map((entry) => ({
        canonical: entry.canonical,
        ticker: entry.ticker,
        aliases: Array.isArray(entry.aliases)
          ? entry.aliases.filter((alias) => typeof alias === "string" && alias.trim())
          : []
      }));
  } catch {
    return [...defaultDictionary];
  }
}

function persistDictionary() {
  localStorage.setItem(STORAGE_KEYS.dictionary, JSON.stringify(state.dictionary));
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function escapeAttribute(value) {
  return escapeHtml(value).replaceAll("`", "");
}
