Run: venv\Scripts\python parser.py

# Parser Script Guide

This document explains how to run `parser.py` using a virtual environment (`venv`).

## 1. Prerequisites
- Project files:
  - `parser.py`
  - `requirements.txt`
  - `jobs_raw.json` (input data)

---

## 2. Setup Virtual Environment

### 2.1. Create a virtual environment:

```bash
python -m venv venv
```

### 2.2 Activate vitual environment:
### Window
```bash
  venv\Scripts\activate
```
### Linux / MacOS
```bash
  source venv/bin/activate
```

### 2.3. Deactive
```bash
  deactivate
``` 

---
## 3. Install + Run
### 3.1. Install required Python packages:

```bash
pip install -r requirements.txt
```

### 3.2. Run
```bash
  python parser.py
```
