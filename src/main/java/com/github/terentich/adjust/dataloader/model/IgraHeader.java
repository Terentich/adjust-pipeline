package com.github.terentich.adjust.dataloader.model;

import java.util.Objects;

public final class IgraHeader {
    private final String id;
    private final int year;
    private final int month;
    private final int day;
    private final int hour;
    private final int reltime;
    private final int numlev;
    private final String psrc;
    private final String npsrc;
    private final int lat;
    private final int lon;

    public IgraHeader(String id,
                      int year, int month, int day, int hour,
                      int reltime, int numlev,
                      String psrc, String npsrc,
                      int lat, int lon) {
        this.id = id;
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.reltime = reltime;
        this.numlev = numlev;
        this.psrc = psrc;
        this.npsrc = npsrc;
        this.lat = lat;
        this.lon = lon;
    }

    public String id() {
        return id;
    }

    public int year() {
        return year;
    }

    public int month() {
        return month;
    }

    public int day() {
        return day;
    }

    public int hour() {
        return hour;
    }

    public int reltime() {
        return reltime;
    }

    public int numlev() {
        return numlev;
    }

    public String psrc() {
        return psrc;
    }

    public String npsrc() {
        return npsrc;
    }

    public int lat() {
        return lat;
    }

    public int lon() {
        return lon;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (IgraHeader) obj;
        return Objects.equals(this.id, that.id) &&
               this.year == that.year &&
               this.month == that.month &&
               this.day == that.day &&
               this.hour == that.hour &&
               this.reltime == that.reltime &&
               this.numlev == that.numlev &&
               Objects.equals(this.psrc, that.psrc) &&
               Objects.equals(this.npsrc, that.npsrc) &&
               this.lat == that.lat &&
               this.lon == that.lon;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, year, month, day, hour, reltime, numlev, psrc, npsrc, lat, lon);
    }

    @Override
    public String toString() {
        return "IgraHeader[" +
               "id=" + id + ", " +
               "year=" + year + ", " +
               "month=" + month + ", " +
               "day=" + day + ", " +
               "hour=" + hour + ", " +
               "reltime=" + reltime + ", " +
               "numlev=" + numlev + ", " +
               "psrc=" + psrc + ", " +
               "npsrc=" + npsrc + ", " +
               "lat=" + lat + ", " +
               "lon=" + lon + ']';
    }

}