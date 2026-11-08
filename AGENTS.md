# AGENTS.md

## Git commits and pull requests

- NEVER sign commits or PRs as co-authored by Claude or any AI model. Do not add `Co-Authored-By:` trailers naming Claude, Anthropic, or any model, and do not add "Generated with Claude Code" lines.
- Commits are authored solely by the configured git user (gabe <gabe@10kweb.co>).

## Docker builds and image tags

- Always build and push image tags for both `linux/amd64` and `linux/arm64` (multi-arch), e.g. `docker buildx build --platform linux/amd64,linux/arm64 -t <dockerhub-user>/mybibliotheca:<tag> --push .`
- Never push a single-architecture tag.
