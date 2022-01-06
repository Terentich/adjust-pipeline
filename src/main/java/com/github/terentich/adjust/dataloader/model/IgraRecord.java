package com.github.terentich.adjust.dataloader.model;

import java.util.Objects;

public final class IgraRecord {
    private final Integer lvltyp1;
    private final Integer lvltyp2;
    private final Integer etime;
    private final Integer press;
    private final String pflag;
    private final Integer gph;
    private final String zflag;
    private final Integer temp;
    private final String tflag;
    private final Integer rh;
    private final Integer dpdp;
    private final Integer wdir;
    private final Integer wspd;

    public IgraRecord(Integer lvltyp1, Integer lvltyp2,
                      Integer etime, Integer press, String pflag,
                      Integer gph, String zflag, Integer temp,
                      String tflag, Integer rh, Integer dpdp, Integer wdir, Integer wspd) {
        this.lvltyp1 = lvltyp1;
        this.lvltyp2 = lvltyp2;
        this.etime = etime;
        this.press = press;
        this.pflag = pflag;
        this.gph = gph;
        this.zflag = zflag;
        this.temp = temp;
        this.tflag = tflag;
        this.rh = rh;
        this.dpdp = dpdp;
        this.wdir = wdir;
        this.wspd = wspd;
    }

    public Integer lvltyp1() {
        return lvltyp1;
    }

    public Integer lvltyp2() {
        return lvltyp2;
    }

    public Integer etime() {
        return etime;
    }

    public Integer press() {
        return press;
    }

    public String pflag() {
        return pflag;
    }

    public Integer gph() {
        return gph;
    }

    public String zflag() {
        return zflag;
    }

    public Integer temp() {
        return temp;
    }

    public String tflag() {
        return tflag;
    }

    public Integer rh() {
        return rh;
    }

    public Integer dpdp() {
        return dpdp;
    }

    public Integer wdir() {
        return wdir;
    }

    public Integer wspd() {
        return wspd;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (IgraRecord) obj;
        return Objects.equals(this.lvltyp1, that.lvltyp1) &&
               Objects.equals(this.lvltyp2, that.lvltyp2) &&
               Objects.equals(this.etime, that.etime) &&
               Objects.equals(this.press, that.press) &&
               Objects.equals(this.pflag, that.pflag) &&
               Objects.equals(this.gph, that.gph) &&
               Objects.equals(this.zflag, that.zflag) &&
               Objects.equals(this.temp, that.temp) &&
               Objects.equals(this.tflag, that.tflag) &&
               Objects.equals(this.rh, that.rh) &&
               Objects.equals(this.dpdp, that.dpdp) &&
               Objects.equals(this.wdir, that.wdir) &&
               Objects.equals(this.wspd, that.wspd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lvltyp1, lvltyp2, etime, press, pflag, gph, zflag, temp, tflag, rh, dpdp, wdir, wspd);
    }

    @Override
    public String toString() {
        return "IgraRecord[" +
               "lvltyp1=" + lvltyp1 + ", " +
               "lvltyp2=" + lvltyp2 + ", " +
               "etime=" + etime + ", " +
               "press=" + press + ", " +
               "pflag=" + pflag + ", " +
               "gph=" + gph + ", " +
               "zflag=" + zflag + ", " +
               "temp=" + temp + ", " +
               "tflag=" + tflag + ", " +
               "rh=" + rh + ", " +
               "dpdp=" + dpdp + ", " +
               "wdir=" + wdir + ", " +
               "wspd=" + wspd + ']';
    }

}
