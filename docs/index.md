---
layout: default
title: Air Quality Forecasting
---

## Models

<div class="models-grid">
  <div class="model-card">
    <div class="model-header">
      <h2>Standard Model</h2>
    </div>
    <div class="model-body">
      <p>7-day PM2.5 forecasts using weather features (temperature, precipitation, wind speed and direction).</p>
      <a href="./air_quality/" class="model-link">View Dashboard →</a>
    </div>
  </div>
  
  <div class="model-card">
    <div class="model-header">
      <h2>Lagged Features Model</h2>
    </div>
    <div class="model-body">
      <p>Enhanced model including historical PM2.5 values from the previous 1-3 days as additional features.</p>
      <a href="./air_quality_lagged/" class="model-link">View Dashboard →</a>
    </div>
  </div>
</div>

<div class="info-section">
  <h2>About</h2>
  <p>This system demonstrates automated ML workflows for air quality forecasting:</p>
  <ul>
    <li>Daily feature engineering from AQICN.org and Open-Meteo.com</li>
    <li>XGBoost regression models trained on historical data</li>
    <li>Automated batch inference and hindcast evaluation</li>
    <li>GitHub Actions orchestration with Hopsworks feature store</li>
  </ul>
</div>

---

<div style="text-align: center; padding: 1.5em 0; color: #6b7280; font-size: 0.9em;">
  <p>Updated daily via automated pipelines</p>
</div>
