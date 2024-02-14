DROP TABLE IF EXISTS act CASCADE;
DROP TABLE IF EXISTS act_gig CASCADE;
DROP TABLE IF EXISTS gig_ticket CASCADE;
DROP TABLE IF EXISTS gig CASCADE;
DROP TABLE IF EXISTS venue CASCADE;
DROP TABLE IF EXISTS ticket CASCADE;

CREATE TABLE venue(
    venueid SERIAL PRIMARY KEY,
    venuename VARCHAR(100),
    hirecost INTEGER,
    capacity INTEGER
);

CREATE TABLE act(
    actID SERIAL PRIMARY KEY,
    actname VARCHAR(100),
    genre VARCHAR(20),
    members INTEGER,
    standardfee INTEGER
);

CREATE TABLE gig(
    gigID SERIAL PRIMARY KEY,
    venueid INTEGER REFERENCES venue(venueid),
    gigtitle VARCHAR(100),
    gigdate TIMESTAMP,
    gigstatus VARCHAR(10), 
    CHECK (gigstatus ILIKE 'Cancelled' or gigstatus ILIKE 'GoingAhead')
);

CREATE TABLE act_gig(
    actID INTEGER REFERENCES act(actID),
    gigID INTEGER REFERENCES gig(gigID),
    actfee INTEGER,
    ontime TIMESTAMP,
    duration INTEGER CHECK(duration >= 0),
    CHECK((ontime + duration * interval'1 minute')::DATE = ontime::DATE)
);

CREATE TABLE gig_ticket(
    gigID INTEGER REFERENCES gig(gigID),
    pricetype VARCHAR(2),
    cost INTEGER
);

CREATE TABLE ticket(
    ticketid SERIAL PRIMARY KEY,
    gigID INTEGER REFERENCES gig(gigID),
    pricetype VARCHAR(2),
    cost INTEGER,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100)
);

 --Takes in venuename
 --Returns venueid
 CREATE OR REPLACE FUNCTION retrieveVID(vName VARCHAR(100))
    RETURNS INTEGER
    LANGUAGE plpgsql AS $$
    DECLARE
        vID INTEGER;
    BEGIN
        SELECT venueid FROM venue WHERE venuename = vName INTO vID;
        return vID;
    END 
    $$;

 --Takes in gigdate
 --Returns gigID
 CREATE OR REPLACE FUNCTION retrieveGID(gStartDate TIMESTAMP)
    RETURNS INTEGER
    LANGUAGE plpgsql AS $$
    DECLARE
        gID INTEGER;
    BEGIN
        SELECT gigID FROM gig WHERE gigdate = gStartDate INTO gID;
        return gID;
    END 
    $$;

 /* Option 1 */ 
 --Takes in ontime and duration act
 --Returns offtime of the act
CREATE OR REPLACE FUNCTION offtime(ontime TIMESTAMP, duration INTEGER)
    RETURNS TIMESTAMP
    LANGUAGE plpgsql AS $$
    DECLARE
        ooftime TIMESTAMP;
    BEGIN
        SELECT (ontime + (duration * interval '1 minute'))::TIMESTAMP into ooftime;
        return ooftime;
    END;
    $$;

 /* Option 2 */ 
 --Takes in venueID, gigTitle, gigDate, Cost of ticket
 CREATE OR REPLACE PROCEDURE OptionTWO (vID INTEGER, gTitle VARCHAR(100), gDate TIMESTAMP, aCost INTEGER) 
    LANGUAGE plpgsql AS $$
    DECLARE
        gID INTEGER;
    BEGIN 
        INSERT INTO gig VALUES (DEFAULT, vID, gTitle, gDate, 'GoingAhead') ON CONFLICT DO NOTHING;
        SELECT gigID FROM gig WHERE venueid = vID AND gigdate = gDate INTO gID;
        INSERT INTO gig_ticket VALUES (gID, 'A', aCost) ON CONFLICT DO NOTHING;
    END 
    $$;

 --Getting a timetable for the venue consisting the times for the venue 
 CREATE OR REPLACE VIEW venueTimeVIEW AS SELECT venueid, ontime, offtime(ontime, duration) as offTime 
    FROM act_gig JOIN gig USING(gigID) ORDER BY venueid;
 --Checking if the new gig overlaps with any existing gig happening in the venue 
 CREATE OR REPLACE VIEW overLapVIEW AS SELECT venueid, (ontime, offTime) overlaps (LAG(ontime) OVER(ORDER BY venueid, ontime), LAG(offTime) OVER(ORDER BY venueid, ontime)) AS overlapss
    FROM venueTimeVIEW;

  --Getting a timetable for all the acts in the gig with ooftime being the difference between the ontime of current act and offtime
 CREATE OR REPLACE VIEW ooftimeVIEW AS SELECT gigID, actID, duration, ontime, ontime::TIME - LAG(offtime(ontime, duration)::TIME)OVER (ORDER BY ontime ASC)::TIME AS ooftime 
    FROM act_gig;

 CREATE OR REPLACE VIEW optionTWOVIEW AS SELECT gigID, ontime, ooftime FROM ooftimeVIEW WHERE ooftime is not null;
 
 --Takes in gigDate and venuename of the gig 
 --Returns whether if the gig being added is valid 
 CREATE OR REPLACE FUNCTION available(gStartDate TIMESTAMP, venueNAM VARCHAR(100))
    RETURNS BOOLEAN
    LANGUAGE plpgsql AS $$
    DECLARE
        vID INTEGER;
        gID INTEGER;
        counte INTEGER;
        maxi BOOLEAN;
        mini BOOLEAN;
        lastActDay DATE;
        firstActDay DATE;
    BEGIN
        SELECT COUNT(*) FROM overLapVIEW WHERE overlapss is true INTO counte;
        SELECT max(ooftime) > INTERVAL '20 minutes' FROM optionTWOVIEW WHERE ontime::TIME != gStartDate::TIME AND gigID = gID INTO maxi;
        SELECT min(ooftime) < INTERVAL '0 minutes' FROM optionTWOVIEW WHERE ontime::TIME != gStartDate::TIME AND gigID = gID INTO mini;
        SELECT gigID FROM gig JOIN venue USING(venueid) WHERE gigdate = gStartDate AND venue.venuename = venueNAM INTO gID;
        SELECT offtime(ontime, duration)::DATE FROM ooftimeVIEW WHERE gigID = gID ORDER BY ontime DESC LIMIT 1 INTO lastActDay;
        SELECT ontime::DATE FROM ooftimeVIEW WHERE gigID = gID ORDER BY ontime ASC LIMIT 1 INTO firstActDay; 
        IF counte > 0 THEN
            return false;
        END IF;
        IF lastActDay != firstActDay THEN
            return false;
        END IF;
        IF mini or maxi THEN
            return false;
        END IF;
        return true;
    END 
    $$;

 /* Option 3 */  
 --Takes in gigID, priceType, CustomerName, CustomerEmail
 --Insert the ticket if all conditions are met 
 CREATE OR REPLACE PROCEDURE OptionTHREE (gID INTEGER, priceTy VARCHAR(2), cName VARCHAR(100), cEmail VARCHAR(100)) 
    LANGUAGE plpgsql AS $$
    DECLARE
        gigCount INTEGER;
        gigCost INTEGER;
        avCapa INTEGER;
        ticketSold INTEGER;
        validity INTEGER := 0;
        firstCMail VARCHAR(100);
        firstName VARCHAR(100);
    BEGIN
        SELECT cost FROM gig_ticket WHERE gigID = gID AND pricetype = priceTy INTO gigCost; 
        SELECT capacity FROM venue JOIN gig USING(venueid) WHERE gigID = gID INTO avCapa;
        SELECT CustomerEmail FROM ticket WHERE CustomerName = cName ORDER BY ticketid ASC LIMIT 1 INTO firstCMail;
        SELECT CustomerName FROM ticket WHERE CustomerEmail = cEmail ORDER BY ticketid ASC LIMIT 1 INTO firstName;
        SELECT count(*) FROM ticket WHERE gigID = gID INTO ticketSold;
        SELECT count(*) FROM gig WHERE gigID = gID INTO gigCount;
        IF gigCount > 0 THEN
            validity := validity + 1;
        END IF;
        IF ticketSold < avCapa THEN 
            validity := validity + 1;
        END IF;
        IF firstCMail = cEmail OR firstCMail is null THEN 
            validity := validity + 1;
        END IF;
        IF firstName = cName OR firstName is null THEN 
            validity := validity + 1;
        END IF;
        IF validity = 4 THEN
            INSERT INTO ticket VALUES(DEFAULT, gID, priceTy, gigCost, cName, cEmail) ON CONFLICT DO NOTHING;
        END IF;
    END 
    $$;

  /* Option 4 */
 --Takes in gigID and actname
 --Cancels the gig if it does not meet the requirements
 CREATE OR REPLACE PROCEDURE OptionFOUR (gID INTEGER, aName VARCHAR(100)) 
    LANGUAGE plpgsql AS $$
    DECLARE
        invalidAct BOOLEAN;
        deletegig INTEGER := 0;
        deletedActID INTEGER;
        deletedActOntime TIMESTAMP;
        finalActOntime TIMESTAMP;
    BEGIN
        SELECT actID FROM act WHERE actname = aName INTO deletedActID;  
        SELECT max(ooftime) >= INTERVAL '20 minutes' FROM ooftimeVIEW WHERE gigID = gID into invalidAct;
        SELECT max(ontime) FROM act_gig WHERE gigID = gID into finalActOntime;
        SELECT ontime FROM act_gig WHERE gigID = gID AND actID = deletedActID into deletedActOntime;
        DELETE FROM act_gig WHERE gigID = gID AND actID = deletedActID;
        IF finalActOntime = deletedActOntime THEN 
            deletegig := deletegig + 1;
        END IF;
        IF invalidAct THEN 
            deletegig := deletegig + 1;
        END IF;
        IF deletegig > 0 THEN
            UPDATE ticket SET cost = 0 WHERE gigID = gID;
            UPDATE gig SET gigstatus = 'Cancelled' WHERE gigID = gID;
        END IF;
    END 
    $$;

 /* Option 5 */

 --Takes in gigID
--Returns number of tickets needed to breakeven
CREATE OR REPLACE FUNCTION ticketneeded(ID INTEGER)
    RETURNS INTEGER
    LANGUAGE plpgsql AS $$
    DECLARE
        sumfee DECIMAL;
        revenue DECIMAL;
        ticketcost DECIMAL;
        hiringcost DECIMAL;
        ticketreq INTEGER;
    BEGIN
        SELECT hirecost FROM gig JOIN venue USING(venueid) WHERE gig.gigID = ID INTO hiringcost;
        SELECT sum(actfee) FROM act_gig WHERE act_gig.gigID = ID into sumfee;
        SELECT sum(cost) FROM ticket WHERE ticket.gigID = ID into revenue;
        IF revenue is null THEN
            SELECT 0 INTO revenue;
        END IF;
        SELECT cost FROM gig_ticket WHERE gig_ticket.gigID = ID and gig_ticket.pricetype = 'A' into ticketcost; 
        SELECT ceiling((hiringcost + sumfee - revenue)/ticketcost) into ticketreq;
        return ticketreq;
    END;
    $$;

 --Takes in gigID 
 --Retrieves the headline for the given gigID
 CREATE OR REPLACE FUNCTION getHeadline(gID INTEGER)
    RETURNS VARCHAR(100)
    LANGUAGE plpgsql AS $$
    DECLARE
        headliner VARCHAR(100);
    BEGIN
        SELECT actname FROM act_gig JOIN act USING(actID) WHERE gigID = gID ORDER BY ontime DESC LIMIT 1 INTO headliner;
        RETURN headliner;
    END
    $$;
 
 --Takes in gigID and CustomerName
 --Returns the count for the given gigID
 CREATE OR REPLACE FUNCTION geticketCount(gID INTEGER, cName VARCHAR(100))
    RETURNS INTEGER
    LANGUAGE plpgsql AS $$
    DECLARE
        ticketCount INTEGER;
    BEGIN
        SELECT count(*) FROM ticket WHERE gigID = gID AND CustomerName = cName INTO ticketCount;
        RETURN ticketCount;
    END
    $$;

 CREATE OR REPLACE VIEW headLineVIEW AS SELECT DISTINCT getHeadline(gigID) AS ActName, EXTRACT(YEAR FROM gigdate) AS YearPlayed 
    FROM gig  
    WHERE gigstatus = 'GoingAhead';

 CREATE OR REPLACE VIEW headLineCustVIEW AS SELECT DISTINCT getHeadline(gigID) AS ActName, CustomerName, EXTRACT(YEAR FROM gigdate) AS YearPlayed, geticketCount(gigID, CustomerName) AS ticketCount
    FROM gig LEFT JOIN ticket USING(gigID) 
    WHERE gig.gigstatus = 'GoingAhead' ORDER BY ActName ASC, ticketCount DESC;

 /* Option 6 */ 
 CREATE OR REPLACE VIEW optionSIXticketsVIEW AS SELECT ActName, YearPlayed::VARCHAR, COUNT(*) AS TOTAL
    FROM headLineCustVIEW WHERE CustomerName is not null GROUP BY YearPlayed, ActName ORDER BY ActName ASC, YearPlayed ASC;

 --Takes in actName 
 --Returns the total number of ticket sold by that act
 CREATE OR REPLACE FUNCTION headLineTicketCount(aName VARCHAR(100))
    RETURNS INTEGER
    LANGUAGE plpgsql AS $$
    DECLARE
        tickets INTEGER;
    BEGIN
        SELECT sum(total) FROM optionSIXticketsVIEW WHERE ActName = aName into tickets;
        RETURN tickets;
    END
    $$;

 CREATE OR REPLACE VIEW optionSIXVIEW AS SELECT COALESCE(ActName, 'Total') AS ActName, COALESCE(YearPlayed, 'Total') AS YearPlayed, sum(Total) 
    FROM optionSIXticketsVIEW 
    GROUP BY ROLLUP(ActName, YearPlayed) ORDER BY headLineTicketCount(ActName) ASC, ActName ASC, YearPlayed ASC;

 /* Option 7 */
 --Takes in CustomerName, ActName 
 --Returns CustomerName if he has at least attend all the acts every yr once
 CREATE OR REPLACE FUNCTION optionSEVEN(cName VARCHAR(100), aName VARCHAR(100))
    RETURNS VARCHAR(100)
    LANGUAGE plpgsql AS $$
    DECLARE
        yearsPlayed INTEGER;
        timesAttended INTEGER;
        custNAME VARCHAR(100);
    BEGIN
        SELECT count(*) FROM headlineview WHERE ActName = aName INTO yearsPlayed;
        SELECT count(*) FROM headLineCustVIEW WHERE ActName = aName AND CustomerName = cName INTO timesAttended;
        IF cName is null THEN
            RETURN '[None]';
        END IF;
        IF yearsPlayed <= timesAttended THEN 
            RETURN cName;
        END IF;
        RETURN null;
    END
    $$;

 CREATE OR REPLACE VIEW optionSEVENVIEW AS SELECT DISTINCT ActName, optionSEVEN(CustomerName, ActName) 
    FROM headLineCustVIEW
    WHERE optionSEVEN(CustomerName, ActName) IS NOT null;

 /* Option 8 */
 --Takes in venuename, actname 
 --Returns tickets required for the organisers to break even
 CREATE OR REPLACE FUNCTION OptionEIGHT(vName VARCHAR(100), aName VARCHAR(100))
    RETURNS INTEGER
    LANGUAGE plpgsql AS $$
    DECLARE    
        hiringCost INTEGER;
        vCapa INTEGER;
        actingCost INTEGER;
        avgFee INTEGER;
        tickets INTEGER;
    BEGIN
        SELECT hirecost FROM venue WHERE venuename = vName INTO hiringCost;
        SELECT capacity FROM venue WHERE venuename = vName INTO vCapa;
        SELECT standardfee FROM act WHERE actname = aName INTO actingCost;
        SELECT avg(cost) FROM ticket LEFT JOIN gig USING(gigID) WHERE gigstatus = 'GoingAhead' INTO avgFee;
        SELECT ceiling((hiringCost + actingCost)/avgFee) INTO tickets;
        IF tickets > vCapa THEN
            RETURN null;
        END IF;
        RETURN tickets;
    END
    $$;
    
 CREATE OR REPLACE VIEW OptionEIGHTVIEW AS SELECT venuename, actname, OptionEIGHT(venuename, actname) 
    FROM act CROSS JOIN venue WHERE OptionEIGHT(venuename, actname) IS NOT null 
    ORDER BY venuename ASC, OptionEIGHT(venuename, actname) DESC;

