---
layout: default
title: Air Quality Dashboard (Lagged Features)
---

<div class="dashboard-header">
  <h1>Air Quality Forecast</h1>
  <p>Lagged Features Model</p>
</div>

<div class="feature-note">
  <strong>Model Enhancement:</strong> Includes PM2.5 values from the previous 1-3 days as additional features to capture temporal patterns.
</div>

## Air Quality Index (AQI)

<div class="aqi-legend">
  <div class="aqi-item aqi-good">Good (0-50)</div>
  <div class="aqi-item aqi-moderate">Moderate (51-100)</div>
  <div class="aqi-item aqi-unhealthy-sensitive">Unhealthy for Some (101-150)</div>
  <div class="aqi-item aqi-unhealthy">Unhealthy (151-200)</div>
  <div class="aqi-item aqi-very-unhealthy">Very Unhealthy (201-300)</div>
  <div class="aqi-item aqi-hazardous">Hazardous (301+)</div>
</div>

---

<div class="section">
  <h2>7-Day Forecast</h2>
  <p>Predicted PM2.5 levels using weather features and historical PM2.5 values.</p>
  
  <div class="image-container">
    <img src="./assets/img/pm25_forecast.png" alt="PM2.5 Forecast">
  </div>
</div>

<div class="section">
  <h2>Model Performance</h2>
  <p>Hindcast comparison of predicted vs. actual PM2.5 measurements.</p>
  
  <div class="image-container">
    <img src="./assets/img/pm25_hindcast_1day.png" alt="PM2.5 Hindcast">
  </div>
</div>

---

<div style="text-align: center; padding: 1em 0; color: #6b7280; font-size: 0.85em;">
  <p>Data: AQICN.org, Open-Meteo.com | Updated daily</p>
</div>