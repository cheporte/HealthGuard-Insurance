# HealthGuard Insurance

An ML-powered web application for health risk assessment and obesity level prediction using machine learning.

This repository is a continuation and practical implementation of the machine learning concepts explored in [ML NULP Course](https://github.com/cheporte/ml-nulp-course). It applies the learned techniques in a real-world application for obesity level prediction.

## Overview

HealthGuard Insurance is a FastAPI-based web application that uses machine learning (CatBoost model) to predict obesity levels based on various lifestyle and physical parameters. The application provides a user-friendly interface for data input and real-time predictions.

## Project Structure

```tree
HealthGuard-Insurance/
├── app/
│   ├── endpoints/         # FastAPI route handlers
│   │   ├── inference.py   # Prediction endpoint
│   │   └── training.py    # Model training endpoint
│   ├── services/         # Business logic
│   │   ├── model.py
│   │   ├── preprocessing.py
│   │   └── utils.py
│   ├── templates/       # HTML templates
│   │   └── index.html
│   └── main.py         # Application entry point
├── data/
│   ├── db/            # Database files
│   └── ObesityDataSet_raw_and_data_sinthetic.csv
├── models/            # Trained ML models
└── requirements.txt
```

## Features

- Web-based interface for data input
- Real-time obesity level prediction
- Data preprocessing pipeline
- Database logging of predictions and inputs
- Input validation and error handling

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/cheporte/HealthGuard-Insurance.git
   cd HealthGuard-Insurance
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   .\venv\Scripts\activate  # Windows
   source venv/bin/activate  # Unix/MacOS
   ```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

1. Start the FastAPI server:

   ```bash
   uvicorn app.main:app --reload
   ```

2. Open your browser and navigate to:

   ```plaintext
   http://localhost:8000/api/v1/risk/predict
   ```

3. Fill in the form with the required information and submit to get a prediction.

## Roadmap

### Short-term Plans

1. **Code Refactoring**
   - Improve code organization and modularity
   - Enhance error handling
   - Add comprehensive logging
   - Implement better type hints
   - Status: Planned

## Tech Stack

- FastAPI
- SQLite
- CatBoost
- Pandas
- NumPy
- Jinja2 Templates
