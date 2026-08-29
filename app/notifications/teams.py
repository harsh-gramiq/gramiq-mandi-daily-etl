"""Microsoft Teams notification public interface."""

from app.pipeline import build_teams_adaptive_card, send_teams_notification

__all__ = ["build_teams_adaptive_card", "send_teams_notification"]
