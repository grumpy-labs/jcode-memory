use crate::message::{Message, ToolDefinition};
use crate::provider::{EventStream, Provider};
use anyhow::Result;
use std::sync::Arc;

/// Provider for runtime modes that need tools, memory, and broker state but not LLM completion.
pub struct NoModelProvider {
    name: &'static str,
}

impl NoModelProvider {
    pub fn broker() -> Self {
        Self {
            name: "broker-no-model",
        }
    }
}

#[async_trait::async_trait]
impl Provider for NoModelProvider {
    async fn complete(
        &self,
        _messages: &[Message],
        _tools: &[ToolDefinition],
        _system: &str,
        _resume_session_id: Option<&str>,
    ) -> Result<EventStream> {
        anyhow::bail!(
            "{} does not allow model completion; start broker serve with --provider/--model to enable live LLM calls",
            self.name
        );
    }

    fn name(&self) -> &str {
        self.name
    }

    fn model(&self) -> String {
        "none".to_string()
    }

    fn fork(&self) -> Arc<dyn Provider> {
        Arc::new(Self { name: self.name })
    }

    fn available_models_display(&self) -> Vec<String> {
        Vec::new()
    }

    async fn prefetch_models(&self) -> Result<()> {
        Ok(())
    }
}
