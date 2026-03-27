//! Connector for Cursor session logs.
//!
//! This wrapper keeps the upstream Cursor composer/state.vscdb support from
//! `franken_agent_detection`, and augments it with direct scanning of Cursor
//! `agent-transcripts/*.jsonl` files. That makes CASS aware of the same raw
//! Cursor pilot transcripts that AIPilot's `/retro` fallback uses.

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde_json::{Value, json};
use walkdir::WalkDir;

use crate::connectors::{
    Connector, DetectionResult, NormalizedConversation, NormalizedMessage, ScanContext,
    file_modified_since, flatten_content, franken_detection_for_connector, reindex_messages,
};

type UpstreamCursorConnector = franken_agent_detection::connectors::cursor::CursorConnector;

pub struct CursorConnector {
    upstream: UpstreamCursorConnector,
}

impl Default for CursorConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorConnector {
    pub fn new() -> Self {
        Self {
            upstream: UpstreamCursorConnector::new(),
        }
    }

    fn cursor_projects_root() -> Option<PathBuf> {
        dirs::home_dir().map(|home| home.join(".cursor").join("projects"))
    }

    fn looks_like_transcript_base(path: &Path) -> bool {
        path.file_name().and_then(|name| name.to_str()) == Some("projects")
            || path.join("agent-transcripts").exists()
    }

    fn transcript_roots(ctx: &ScanContext) -> Vec<PathBuf> {
        let mut roots = BTreeSet::new();
        if !ctx.use_default_detection() {
            // Explicit scan roots take precedence.
            for root in &ctx.scan_roots {
                roots.insert(root.path.clone());
            }
        } else if Self::looks_like_transcript_base(&ctx.data_dir) {
            roots.insert(ctx.data_dir.clone());
        } else {
            // Default detection: look for a `projects` sibling next to the
            // data_dir (e.g. data_dir = ~/.cursor/User/globalStorage →
            // ~/.cursor/projects). Only fall back to the absolute home
            // location when data_dir is the real Cursor root.
            let candidate = ctx.data_dir.parent().and_then(|p| p.parent()).map(|p| p.join("projects"));
            if let Some(ref c) = candidate {
                if c.exists() {
                    roots.insert(c.clone());
                }
            }
            // Also check the canonical location, but only when the data_dir
            // is actually under ~/.cursor (avoids polluting test runs with
            // real transcripts when data_dir is a temp dir).
            if let Some(real_root) = Self::cursor_projects_root() {
                if ctx.data_dir.starts_with(
                    real_root.parent().unwrap_or(&real_root)
                ) {
                    roots.insert(real_root);
                }
            }
        }
        roots.into_iter().collect()
    }

    fn transcript_files(root: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        if !root.exists() {
            return files;
        }

        for entry in WalkDir::new(root)
            .follow_links(true)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            let path = entry.path();
            if !entry.file_type().is_file() {
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
                continue;
            }
            if path
                .ancestors()
                .any(|ancestor| ancestor.file_name().and_then(|n| n.to_str()) == Some("agent-transcripts"))
            {
                files.push(path.to_path_buf());
            }
        }

        files.sort();
        files
    }

    fn workspace_slug(path: &Path) -> Option<String> {
        let parts: Vec<_> = path.components().collect();
        for (idx, part) in parts.iter().enumerate() {
            if part.as_os_str() == "agent-transcripts" && idx > 0 {
                return Some(parts[idx - 1].as_os_str().to_string_lossy().to_string());
            }
        }
        None
    }

    fn system_time_millis(ts: SystemTime) -> Option<i64> {
        ts.duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_millis()).ok())
    }

    fn first_line_summary(text: &str) -> Option<String> {
        let cleaned = text
            .replace("<user_query>", " ")
            .replace("</user_query>", " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if cleaned.is_empty() {
            return None;
        }
        let mut summary = cleaned.chars().take(100).collect::<String>();
        if cleaned.chars().count() > 100 {
            summary.push_str("...");
        }
        Some(summary)
    }

    fn parse_agent_transcript(
        path: &Path,
        since_ts: Option<i64>,
    ) -> Result<Option<NormalizedConversation>> {
        if !file_modified_since(path, since_ts) {
            return Ok(None);
        }

        let metadata = fs::metadata(path)?;
        let started_at = metadata.created().ok().and_then(Self::system_time_millis);
        let ended_at = metadata.modified().ok().and_then(Self::system_time_millis);
        let timestamp_source = if started_at.is_some() {
            "filesystem_created_modified"
        } else {
            "filesystem_modified_only"
        };

        let raw = fs::read_to_string(path)?;
        let mut messages = Vec::new();
        let mut first_user_summary: Option<String> = None;

        for line in raw.lines() {
            let payload: Value = match serde_json::from_str(line) {
                Ok(value) => value,
                Err(_) => continue,
            };

            let role = payload
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or("assistant")
                .to_string();
            let content_value = payload
                .get("message")
                .and_then(|message| message.get("content"))
                .cloned()
                .unwrap_or(Value::Null);
            let content = flatten_content(&content_value);
            if content.trim().is_empty() {
                continue;
            }

            if role == "user" && first_user_summary.is_none() {
                first_user_summary = Self::first_line_summary(&content);
            }

            messages.push(NormalizedMessage {
                idx: messages.len() as i64,
                role,
                author: None,
                created_at: None,
                content,
                extra: payload,
                snippets: Vec::new(),
            });
        }

        if messages.is_empty() {
            return Ok(None);
        }

        reindex_messages(&mut messages);

        let workspace_slug = Self::workspace_slug(path);
        let title = first_user_summary
            .clone()
            .or_else(|| messages.first().and_then(|msg| Self::first_line_summary(&msg.content)));

        Ok(Some(NormalizedConversation {
            agent_slug: "cursor".to_string(),
            external_id: Some(path.file_stem().unwrap_or_default().to_string_lossy().to_string()),
            title,
            workspace: None,
            source_path: path.to_path_buf(),
            started_at: started_at.or(ended_at),
            ended_at,
            metadata: json!({
                "source": "cursor_agent_transcript",
                "workspace_slug": workspace_slug,
                "timestamp_source": timestamp_source,
            }),
            messages,
        }))
    }
}

impl Connector for CursorConnector {
    fn detect(&self) -> DetectionResult {
        let mut detection =
            franken_detection_for_connector("cursor").unwrap_or_else(DetectionResult::not_found);

        if let Some(root) = Self::cursor_projects_root() {
            let transcript_detected = root.exists()
                && Self::transcript_files(&root)
                    .into_iter()
                    .next()
                    .is_some();
            if transcript_detected {
                detection.detected = true;
                detection
                    .evidence
                    .push(format!("Cursor agent transcripts found under {}", root.display()));
                if !detection.root_paths.contains(&root) {
                    detection.root_paths.push(root);
                }
            }
        }

        detection
    }

    fn scan(&self, ctx: &ScanContext) -> Result<Vec<NormalizedConversation>> {
        let mut conversations = self.upstream.scan(ctx)?;
        let mut seen_paths: HashSet<PathBuf> = conversations
            .iter()
            .map(|conversation| conversation.source_path.clone())
            .collect();

        for root in Self::transcript_roots(ctx) {
            for transcript in Self::transcript_files(&root) {
                if let Some(conversation) = Self::parse_agent_transcript(&transcript, ctx.since_ts)?
                    && seen_paths.insert(conversation.source_path.clone())
                {
                    conversations.push(conversation);
                }
            }
        }

        Ok(conversations)
    }
}
