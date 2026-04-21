// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::ffi::OsString;

use paro_context::StatementAuthContext;

const CREATE_ROUTINE_USERS_ENV: &str = "PARO_CREATE_ROUTINE_USERS";
const CREATE_ELEVATED_ROUTINE_USERS_ENV: &str = "PARO_CREATE_ELEVATED_ROUTINE_USERS";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionAuthPolicy {
    create_routine_users: HashSet<String>,
    create_elevated_routine_users: HashSet<String>,
}

impl Default for SessionAuthPolicy {
    fn default() -> Self {
        let mut defaults = HashSet::new();
        defaults.insert("paro".to_string());
        Self {
            create_routine_users: defaults.clone(),
            create_elevated_routine_users: defaults,
        }
    }
}

impl SessionAuthPolicy {
    pub(crate) fn from_env() -> Self {
        Self::from_env_values(
            std::env::var_os(CREATE_ROUTINE_USERS_ENV),
            std::env::var_os(CREATE_ELEVATED_ROUTINE_USERS_ENV),
        )
    }

    fn from_env_values(
        create_routine_users: Option<OsString>,
        create_elevated_routine_users: Option<OsString>,
    ) -> Self {
        let create_routine_users = parse_user_set(create_routine_users);
        let create_elevated_routine_users = parse_user_set(create_elevated_routine_users);

        let creators = create_routine_users.unwrap_or_else(|| {
            let mut defaults = HashSet::new();
            defaults.insert("paro".to_string());
            defaults
        });
        let elevated = create_elevated_routine_users.unwrap_or_else(|| creators.clone());

        Self {
            create_routine_users: creators,
            create_elevated_routine_users: elevated,
        }
    }

    pub(crate) fn auth_context_for_user(&self, user_name: &str) -> StatementAuthContext {
        let normalized = normalize_user(user_name);
        StatementAuthContext {
            authenticated_user: Some(user_name.to_string()),
            can_create_routine: self.create_routine_users.contains(&normalized),
            can_create_elevated_routine: self.create_elevated_routine_users.contains(&normalized),
            ..StatementAuthContext::default()
        }
    }
}

fn parse_user_set(raw: Option<OsString>) -> Option<HashSet<String>> {
    raw.map(|value| {
        value
            .to_string_lossy()
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(normalize_user)
            .collect()
    })
}

fn normalize_user(user_name: impl AsRef<str>) -> String {
    user_name.as_ref().trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::SessionAuthPolicy;

    #[test]
    fn default_policy_grants_paro_full_routine_privilege() {
        let policy = SessionAuthPolicy::default();

        let paro = policy.auth_context_for_user("paro");
        assert!(paro.can_create_routine);
        assert!(paro.can_create_elevated_routine);

        let alice = policy.auth_context_for_user("alice");
        assert!(!alice.can_create_routine);
        assert!(!alice.can_create_elevated_routine);
    }

    #[test]
    fn env_policy_supports_separate_create_and_elevated_sets() {
        let policy = SessionAuthPolicy::from_env_values(
            Some("paro,routine_builder".into()),
            Some("paro".into()),
        );

        let builder = policy.auth_context_for_user("routine_builder");
        assert!(builder.can_create_routine);
        assert!(!builder.can_create_elevated_routine);

        let admin = policy.auth_context_for_user("PARO");
        assert!(admin.can_create_routine);
        assert!(admin.can_create_elevated_routine);
    }

    #[test]
    fn elevated_defaults_to_create_set_when_only_creator_env_is_present() {
        let policy = SessionAuthPolicy::from_env_values(Some("alice,bob".into()), None);

        let alice = policy.auth_context_for_user("alice");
        assert!(alice.can_create_routine);
        assert!(alice.can_create_elevated_routine);
    }
}
