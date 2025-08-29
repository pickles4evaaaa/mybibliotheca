# Copyright and Dependency Policy

This document outlines the copyright and dependency policy for MyBibliotheca to ensure the repository remains free of unwanted copyright notices and unnecessary AI/ML dependencies.

## Copyright Policy

MyBibliotheca is licensed under the MIT License and should only contain:
- Original code created for this project
- Code from dependencies explicitly listed in requirements.txt/pyproject.toml
- Standard open-source libraries compatible with the MIT License

**Unwanted Copyright Notices:**
The repository must not contain any of the following:
- NVIDIA Corporation copyright notices
- References to NVIDIA Deep Learning Container License
- SHMEM allocation warnings related to PyTorch/NVIDIA
- GPU-specific container recommendations

## Dependency Policy

MyBibliotheca is a book management web application and should not include:
- Machine Learning or AI libraries (transformers, torch, tensorflow, etc.)
- Computer vision libraries beyond basic image processing
- Natural Language Processing libraries
- Model hosting or inference frameworks

**Specifically Prohibited:**
- microsoft/VibeVoice or similar pre-trained models
- Tokenizer libraries (Qwen2Tokenizer, etc.)
- HuggingFace transformers
- PyTorch or TensorFlow
- CUDA or GPU-specific libraries

## Checking for Violations

A copyright and dependency checker is available as `.copyright-check.py` (excluded from version control).
To run the checker:

```bash
python3 .copyright-check.py
```

This script will scan all text files in the repository for:
1. Unwanted copyright notices
2. Prohibited AI/ML dependencies
3. References to external model repositories

## Approved Dependencies

Only the following types of dependencies are approved:
- Flask and related web framework components
- Database libraries (SQLAlchemy, etc.)
- Authentication and security libraries
- Basic image processing (Pillow for book covers)
- Testing frameworks (pytest)
- Standard Python utilities

## Enforcement

This policy is enforced through:
1. Manual code review
2. Automated checking via `.copyright-check.py`
3. Dependency auditing during updates

Any violations should be immediately removed and the repository cleaned of unwanted content.

## Contact

If you have questions about this policy or need to add a new dependency, please create an issue to discuss whether it aligns with the project goals.