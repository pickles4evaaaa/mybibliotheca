#!/bin/bash
# Copyright and dependency checker for MyBibliotheca
# Ensures the repository remains free of unwanted copyright notices and AI/ML dependencies

echo "🔍 Checking for unwanted copyright notices and AI/ML dependencies..."

# Count issues found
ISSUES=0

# Check for NVIDIA copyright notices
if grep -r "NVIDIA CORPORATION" . --include="*.py" --include="*.md" --include="*.txt" --include="*.yml" --include="*.yaml" --include="*.json" --include="*.toml" --exclude-dir=".git" --exclude-dir="__pycache__" --exclude="COPYRIGHT_POLICY.md" --exclude=".copyright-check.py" 2>/dev/null; then
    echo "❌ Found NVIDIA CORPORATION copyright notices"
    ISSUES=$((ISSUES + 1))
fi

if grep -r "Deep Learning Container License" . --include="*.py" --include="*.md" --include="*.txt" --include="*.yml" --include="*.yaml" --include="*.json" --include="*.toml" --exclude-dir=".git" --exclude-dir="__pycache__" --exclude="COPYRIGHT_POLICY.md" --exclude=".copyright-check.py" 2>/dev/null; then
    echo "❌ Found NVIDIA Deep Learning Container License references"
    ISSUES=$((ISSUES + 1))
fi

# Check for AI/ML dependencies
if grep -r "VibeVoice\|Qwen2Tokenizer\|transformers\|torch\|tensorflow" . --include="*.py" --include="*.txt" --include="requirements.txt" --include="pyproject.toml" --exclude-dir=".git" --exclude-dir="__pycache__" --exclude="COPYRIGHT_POLICY.md" --exclude=".copyright-check.py" --exclude="test_dependencies.py" 2>/dev/null; then
    echo "❌ Found AI/ML dependencies"
    ISSUES=$((ISSUES + 1))
fi

# Check for preprocessor config references
if grep -r "preprocessor_config\.json" . --include="*.py" --include="*.md" --include="*.txt" --exclude-dir=".git" --exclude-dir="__pycache__" --exclude="COPYRIGHT_POLICY.md" --exclude=".copyright-check.py" 2>/dev/null; then
    echo "❌ Found preprocessor_config.json references"
    ISSUES=$((ISSUES + 1))
fi

if [ $ISSUES -eq 0 ]; then
    echo "✅ No unwanted copyright notices or AI/ML dependencies found"
    exit 0
else
    echo "❌ Found $ISSUES issue(s) - please review and clean the repository"
    exit 1
fi