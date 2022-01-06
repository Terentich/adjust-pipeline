package com.github.terentich.adjust.dataloader.io;

import com.github.terentich.adjust.dataloader.db.DbManager;
import com.github.terentich.adjust.dataloader.model.IgraData;
import com.github.terentich.adjust.dataloader.model.IgraHeader;
import com.github.terentich.adjust.dataloader.model.IgraRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class IgraFileReader {
    private static final Logger log = LoggerFactory.getLogger(IgraFileReader.class);

    private static final String HEADER_INDICATOR = "#";

    private static final int BATCH_SIZE = 1_000_000;
    private static final int MAX_BATCH_DATA_RECORDS = 500;

    /*
        Header record specification:
        -------------------------------
        Variable        Columns Type
        -------------------------------
        HEADREC       1-  1  Character
        ID            2- 12  Character
        YEAR         14- 17  Integer
        MONTH        19- 20  Integer
        DAY          22- 23  Integer
        HOUR         25- 26  Integer
        RELTIME      28- 31  Integer
        NUMLEV       33- 36  Integer
        P_SRC        38- 45  Character
        NP_SRC       47- 54  Character
        LAT          56- 62  Integer
        LON          64- 71  Integer
        ---------------------------------
        Example:
        #USM00070261 1930 08 26 99 1700    6          cdmp-usm  648161 -1478767
    */
    private static final Pattern HEADER_PATTERN = Pattern.compile(
            "#(\\S{11})" +                 // id
            "\\p{Blank}" +
            "(\\d{4})" +                   // year
            "\\p{Blank}" +
            "(\\d{2})" +                   // month
            "\\p{Blank}" +
            "(\\d{2})" +                   // day
            "\\p{Blank}" +
            "(\\d{2})" +                   // hour
            "\\p{Blank}" +
            "(\\s{0,3}\\d{1,4})" +         // reltime
            "\\p{Blank}" +
            "(\\p{Blank}{0,3}\\d{1,4})" +  // numlev
            "\\p{Blank}" +
            "(\\p{Blank}{0,8}\\S{0,8})" +  // p_src
            "\\p{Blank}" +
            "(\\p{Blank}{0,8}\\S{0,8})" +  // np_src
            "\\p{Blank}" +
            "(\\p{Blank}{0,6}\\d{0,7})" +  // lat
            "\\p{Blank}" +
            "(\\p{Blank}{0,6}-\\d{0,7})"   // lon
    );

    /*
        Data record specification:
        -------------------------------
        Variable        Columns Type
        -------------------------------
        LVLTYP1         1-  1   Integer
        LVLTYP2         2-  2   Integer
        ETIME           4-  8   Integer
        PRESS          10- 15   Integer
        PFLAG          16- 16   Character
        GPH            17- 21   Integer
        ZFLAG          22- 22   Character
        TEMP           23- 27   Integer
        TFLAG          28- 28   Character
        RH             29- 33   Integer
        DPDP           35- 39   Integer
        WDIR           41- 45   Integer
        WSPD           47- 51   Integer
        -------------------------------
        Example:
        30 -9999  -9999   250 -9999 -9999 -9999    90    20
        10 -9999  40000  6440B -441B-9999 -9999   180   110
        10    16 100000B  213B -228B  738    34    49    11
        20  9237   1043 30497B -461B    5   392   300   570
        20  7636   3880 22162B -501B-9999 -9999   255   230
        20 10400   1310 29153B -510B-9999 -9999   237   180
        10 11224   1000 31866B -343B-9999 -9999    69    50
    */
    private static final Pattern RECORD_PATTERN = Pattern.compile(
            "(\\d)" +                       // lvltyp1
            "(\\d)" +                       // lvltyp2
            "\\p{Blank}" +
            "(\\p{Blank}{0,5}-?\\d{1,5})" + // etime
            "\\p{Blank}" +
            "(\\p{Blank}{0,5}-?\\d{1,6})" + // press
            "(\\S?)" +                      // pflag
            "(\\p{Blank}{0,5}-?\\d{1,5})" + // gph
            "(\\S?)" +                      // zflag
            "(\\p{Blank}{0,5}-?\\d{1,5})" + // temp
            "(\\S?)" +                      // tflag
            "(\\p{Blank}{0,5}-?\\d{1,5})" + // rh
            "\\p{Blank}" +
            "(\\p{Blank}{0,5}-?\\d{1,5})" + // dpdp
            "\\p{Blank}" +
            "(\\p{Blank}{0,5}-?\\d{1,5})" + // wdir
            "\\p{Blank}" +
            "(\\p{Blank}{0,5}-?\\d{1,5})" + // wspd
            "\\p{Blank}"
    );

    private final DbManager dbManager;

    public IgraFileReader(DbManager dbManager) {
        this.dbManager = dbManager;
    }

    public int processIgraData(File file) {
        int correctLineCount = 0;
        int totalLineCount = 0;
        int savedLineCount = 0;
        int headerLineCount = 0;
        int commitedLineCount = 0;
        List<IgraData> igraData = new ArrayList<>(BATCH_SIZE);

        try (ZipFile zipFile = new ZipFile(file)) {
            ZipEntry zipEntry = zipFile.entries().nextElement();

            try (InputStream stream = zipFile.getInputStream(zipEntry);
                 InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
                 BufferedReader bufferedReader = new BufferedReader(reader)) {

                IgraHeader header = null;
                List<IgraRecord> dataRecords = new ArrayList<>(MAX_BATCH_DATA_RECORDS);
                String line;
                int failedLineCount = 0;

                while ((line = bufferedReader.readLine()) != null) {
                    if (line.startsWith(HEADER_INDICATOR)) {
                        if (totalLineCount == 0) {
                            header = createHeader(line);
                        } else {
                            igraData.add(new IgraData(header, dataRecords));
                            header = createHeader(line);
                            dataRecords = new ArrayList<>(MAX_BATCH_DATA_RECORDS);
                        }

                        if (header == null) {
                            failedLineCount++;
                        } else {
                            headerLineCount++;
                        }

                        if (totalLineCount - headerLineCount - commitedLineCount > BATCH_SIZE) {
                            savedLineCount += dbManager.saveData(igraData);
                            igraData.clear();
                            commitedLineCount = totalLineCount;
                        }
                    } else {
                        IgraRecord record = createRecord(line);

                        if (record == null) {
                            failedLineCount++;
                        } else {
                            dataRecords.add(record);
                        }
                    }

                    totalLineCount++;
                }

                igraData.add(new IgraData(header, dataRecords));
                savedLineCount += dbManager.saveData(igraData);

                log.info("Total file lines = {} (headers: {}), saved lines = {}, failed lines = {} ", totalLineCount,
                        headerLineCount, savedLineCount, failedLineCount);
                correctLineCount = totalLineCount - headerLineCount;
                if (correctLineCount == savedLineCount) {
                    log.info("All file lines have been saved successfully");
                } else {
                    log.error("Some file lines have not been saved: should be {}, but {} (diff={})", correctLineCount,
                            savedLineCount, (correctLineCount - savedLineCount));
                }
            }
        } catch (IOException | SQLException e) {
            log.error("Unable to process input file", e);
        }

        return correctLineCount;
    }

    public IgraHeader createHeader(String headerLine) {
        IgraHeader header = null;
        String[] tokens = parse(headerLine, HEADER_PATTERN, 11);

        try {
            header = new IgraHeader(
                    tokens[0],
                    Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3]), Integer.parseInt(tokens[4]),
                    Integer.parseInt(tokens[5]), Integer.parseInt(tokens[6]),
                    tokens[7], tokens[8],
                    Integer.parseInt(tokens[9]), Integer.parseInt(tokens[10])
            );
        } catch (NumberFormatException e) {
            log.error("Unable to parse header tokens: '{}'\n{}", headerLine, e);
        }

        return header;
    }

    public IgraRecord createRecord(String recordLine) {
        IgraRecord record = null;
        String[] tokens = parse(recordLine, RECORD_PATTERN, 13);

        try {
            record = new IgraRecord(
                    Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]),
                    Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3]), tokens[4],
                    Integer.parseInt(tokens[5]), tokens[6], Integer.parseInt(tokens[7]),
                    tokens[8], Integer.parseInt(tokens[9]), Integer.parseInt(tokens[10]),
                    Integer.parseInt(tokens[11]), Integer.parseInt(tokens[12])
            );
        } catch (NumberFormatException e) {
            log.error("Unable to parse record tokens: '{}'\n{}", recordLine, e);
        }

        return record;
    }

    public String[] parse(String inputLine, Pattern pattern, int requiredTokens) {
        Matcher matcher = pattern.matcher(inputLine);

        if (!matcher.matches() || matcher.groupCount() != requiredTokens) {
            log.error("Unknown line format: '{}'", inputLine);
            return new String[]{};
        }

        String[] tokens = new String[matcher.groupCount()];

        for (int i = 1; i <= matcher.groupCount(); i++) {
            tokens[i - 1] = matcher.group(i).trim();
        }

        return tokens;
    }
}
