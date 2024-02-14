# Description of My Solutions

## **Option 1**
For this option, I did
```sql
SELECT actname, ontime::time, offtime(ontime, duration)::TIME 
FROM act_gig JOIN act USING(actID) WHERE gigID=? ORDER BY ontime
```
to obtain the resultset that satisfies what this problemset asks for. 
```sql
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
```
Is a function I created that takes in the parameter mentioned above and returns the offtime of an act as a variabletype TIMESTAMP which I casted to TIME.

## **Option 2**
### *insertStatement1*
For this option, I started off by setting a savepoint and turning off autocommit and then I called the procedure OptionTwo which inserted the some of the parameters given by the function option2 itself.
```sql
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
```

### *insertStatement2*
Afterwards, I looped through the actIDs[] array and added the parameters for act_gig in a batch. 

### *updateStatement1*
Finally, I used the function available(gigdate, venuename) that I created in schema.sql which checks... 
1. if the gig I just 'added' has any acts that overlaps with the already existing acts in the same venue;
2. if the gig ends on the same day, 
3. if any acts of the gig have a 20 minutes interval or if gig itself has any overlapping acts.

If all of the above conditions were met, then the gig has been successfully added, else we rollback to the initial state of the database before this method was called.
> *P.S. why did I call this prepared statement an updateStatement? Well it sort of 'updates' the current state of the database back to its original state.*

## **Option 3**
To book a ticket, there are various condtions we need to check:
1. If the gig that we are booking a ticket for exists
2. If the venue capacity > the number ticket available
3. If the CustomerName is the same as the one used before as well as the CustomerEmail, else if both field are null means that the customer is booking for the 1st time and so it will go through.

If all above conditions are true than the ticket will be booked. All of this is done in the OptionTHREE function that I created. (see optionTHREE for precise details)


## **Option 4**
In this option, we start off by calling a procedure OptionFOUR(gigID, actname) Where it will check:
1. If the act we are deleting is a headline act, then we cancel the gig.
2. If removing the act will result in a 20 minutes gap between the remaining acts, then we cancel the gig.

As a procedure does not return anything and we are updating the database. We do a query that will select all the CustomerEmails that has been affected by cancelling the gig and we add them to the array for returning.

## **Option 5**
For this option, I created a function named ticketneeeded(gigID) *,to breakeven,* where I performed the calculations *needed* to obtain the number of tickets the organisers are required to sell to breakeven, apart from doing the calculations, the thing that I needed to be careful with is when any of the values required for the calculation ends up being null, which is the case for revenue where there are no tickets currently sold. 

```sql
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
```

## **Option 6**
For this option, I created two functions: 
1. getHeadline(gigID) which returns the headlineact of the given gigID,
2. headLineTicketCount(actname) which returns the number of ticket sold for that headlineact. 

After that I created few VIEWS along with optionSIXVIEW where I COALESCE the years when the act played and the actname with 'Total' and grouped by rollup(actname, year) to achieve the desired format for the solution of this problemset.

```sql
CREATE OR REPLACE VIEW optionSIXVIEW AS SELECT COALESCE(ActName, 'Total') AS ActName, COALESCE(YearPlayed, 'Total') AS YearPlayed, sum(Total) 
    FROM optionSIXticketsVIEW 
    GROUP BY ROLLUP(ActName, YearPlayed) ORDER BY headLineTicketCount(ActName) ASC, ActName ASC, YearPlayed ASC;
```

## **Option 7**
In this option, I used headlineVIEW: 
```sql
CREATE OR REPLACE VIEW headLineVIEW AS SELECT DISTINCT getHeadline(gigID) AS ActName, EXTRACT(YEAR FROM gigdate) AS YearPlayed 
    FROM gig  
    WHERE gigstatus = 'GoingAhead';
```
and headlineCustVIEW:
```sql
CREATE OR REPLACE VIEW headLineCustVIEW AS SELECT DISTINCT getHeadline(gigID) AS ActName, CustomerName, EXTRACT(YEAR FROM gigdate) AS YearPlayed, geticketCount(gigID, CustomerName) AS ticketCount
    FROM gig LEFT JOIN ticket USING(gigID) 
    WHERE gig.gigstatus = 'GoingAhead' ORDER BY ActName ASC, ticketCount DESC;
```
We perform select count(*) on headLineVIEW to find the number of times that act has been a headLineAct and we perform the same on headLineCustVIEW to find the number of times such customer has been to a headlineAct. By comparing these two numbers that we obtain, we are able to find a table consisting of headlineAct and the customernames that attended all of the times such headlineact has been played.

## **Option 8**

This option is very similar to option 5 and we follow the same methodology:
1. Find all of the values required for the calculation,
2. Doing so will result in us finding the number of tickets,
3. Check if the number of ticket > venue capacity,
4. Return the tickets.

In the specification for this problemset, we are asked to order it by proportion, but in the schema.sql, we ordered it by the number of tickets as we are looking at the same venue. 
>Meaning that even if we do find the proportion and order according to it, we are dividing the number of tickets by the same venue capacity, and so ordering by tickets will yield us the results in the same order.

```sql
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
```