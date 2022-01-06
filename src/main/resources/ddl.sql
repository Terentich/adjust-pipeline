DROP TABLE IF EXISTS igra_data;

CREATE TABLE igra_data(
    id      VARCHAR(11) NOT NULL,
    year    SMALLINT    NOT NULL,
    month   SMALLINT    NOT NULL,
    day     SMALLINT    NOT NULL,
    hour    SMALLINT    NOT NULL,
    reltime SMALLINT    NOT NULL,
    numlev  SMALLINT    NOT NULL,
    p_src   VARCHAR(8)  NOT NULL,
    np_src  VARCHAR(8)  NOT NULL,
    lat     INTEGER     NOT NULL,
    lon     INTEGER     NOT NULL,

    lvltyp1 SMALLINT   NOT NULL,
    lvltyp2 SMALLINT   NOT NULL,
    etime   SMALLINT   NOT NULL,
    press   INTEGER    NOT NULL,
    pflag   VARCHAR(1) NOT NULL,
    gph     INTEGER    NOT NULL,
    zflag   VARCHAR(1) NOT NULL,
    temp    SMALLINT   NOT NULL,
    tflag   VARCHAR(1) NOT NULL,
    rh      SMALLINT   NOT NULL,
    dpdp    SMALLINT   NOT NULL,
    wdir    SMALLINT   NOT NULL,
    wspd    SMALLINT   NOT NULL
);
