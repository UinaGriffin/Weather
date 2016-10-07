package com.popova.weather;

import java.io.Serializable;

public class MonthlyRecord implements Serializable{
    private Integer year;
    private Integer month;
    private Double temperatureMax;
    private Double temperatureMin;
    private Integer airFrostDays;
    private Double rain;

    public MonthlyRecord() {
    }

    public Integer getAirFrostDays() {
        return airFrostDays;
    }

    public void setAirFrostDays(Integer airFrostDays) {
        this.airFrostDays = airFrostDays;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    public Double getRain() {
        return rain;
    }

    public void setRain(Double rain) {
        this.rain = rain;
    }

    public Double getTemperatureMax() {
        return temperatureMax;
    }

    public void setTemperatureMax(Double temperatureMax) {
        this.temperatureMax = temperatureMax;
    }

    public Double getTemperatureMin() {
        return temperatureMin;
    }

    public void setTemperatureMin(Double temperatureMin) {
        this.temperatureMin = temperatureMin;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }
}
