/*
In this project, I leveraged BigQuery for data analysis and utilized SQL to
implement a comprehensive approach. To conduct RFM analysis, I carefully selected
pertinent features, calculated RFM scores systematically, and applied a
segmentation methodology based on the derived RFM scores.
*/


/*
Recognizing the limitations of assessing customer value through a single metric,
I implemented the RFM score methodology in this project. By assigning numerical
values based on recency, frequency, and monetary factors, we gain a nuanced
understanding of customer behaviour.

Points are assigned for recent purchases, frequent transactions, and higher
values, resulting in a comprehensive RFM score. This score is pivotal for
segmenting our Customer Data Platform (CDP), offering actionable insights.

Whether using the detailed five bands or a streamlined three based on data
variance, this framework guides strategic decision-making. Drawing inspiration
from the UK Data & Marketing Association's 11 segments, we tailor marketing
strategies to align with unique customer characteristics. This approach ensures
a targeted and effective customer engagement strategy.
*/

/*
[Start] --> [Data Processing] --> [Compute Recency, Frequency, and Monetary Values per Customer]

            --> [Determine Quantiles for Each RFM Metric]
            --> [Assign Scores for Each RFM Metric]
            --> [Define RFM Segments using Scores] --> [End]
*/

--------------------------------------------------------------------------------

select InvoiceNo,StockCode,Quantity,UnitPrice, (Quantity*UnitPrice) AS amount
from `customer-segmentation-405712.retail.sales`

--------------------------------------------------------------------------------

/*
OUTPUT TABLE 1 -----------------------------------------------------------------
*/

/*
After finding the total cost for each product, we proceed to calculate the
amount spent on each visit. Using a query and a Common Table Expression (CTE),
we group by invoice ID and sum the total cost to obtain the actual bill amount.
*/

--------------------------------------------------------------------------------

with bills as
  (
    select InvoiceNo, (Quantity*UnitPrice) AS amount
    from `customer-segmentation-405712.retail.sales`
  )
select InvoiceNo, sum(amount) total
from bills
group by InvoiceNo

--------------------------------------------------------------------------------

/*
OUTPUT TABLE 2 -----------------------------------------------------------------
*/

/*
Will save this data as a table by name of "bill" for further use.
*/


/*
Now I have computed the RFM values

- Monetary Value :
  - Computed as the sum of sales.

- Frequency :
  - Involves counting distinct invoice numbers per customer, indicating the number
    of separate purchases during their customer relationship.
  - Requires identification of the first and last purchase for each customer.

- Recency :
  - Determined by finding the last purchase for each customer.
  - Involves joining the saved 'bill' table with the 'sales' table and adding
    the total cost at the customer level for the monetary value.
*/

--------------------------------------------------------------------------------

select s.CustomerID,
  date(min(s.InvoiceDate)) first_purchase, date(max(s.InvoiceDate)) recent_purchase,
  count(distinct(s.InvoiceDate)) no_of_orders, sum(b.total) monetary
from `customer-segmentation-405712.retail.sales` s
left join `customer-segmentation-405712.retail.bill` b
  on s.InvoiceNo = b.InvoiceNo
group by s.CustomerID

--------------------------------------------------------------------------------

/*
OUTPUT TABLE 3 -----------------------------------------------------------------
*/

/*
In understanding customer behaviour :

- Recency :
  - I opted for a reference date, typically the most recent purchase.
  - We then assessed the time gap between this reference date and each customer's latest purchase.

- Frequency :
  - To gauge customer engagement, we calculated the tenure by examining the
    difference between their initial and latest purchase dates, adding 1 to
    account for instances when the first and last month coincide.
*/

--------------------------------------------------------------------------------

with tb1 as
  (
    select s.CustomerID, date(min(s.InvoiceDate)) first_purchase,
      date(max(s.InvoiceDate)) recent_purchase,
      count(distinct(s.InvoiceDate)) no_of_orders, sum(b.total) monetary
    from `customer-segmentation-405712.retail.sales` s
    left join `customer-segmentation-405712.retail.bill` b
      on s.InvoiceNo = b.InvoiceNo
    group by s.CustomerID
  )

select *, date_diff(reference_date, recent_purchase, DAY) recency,
  no_of_orders/month_cnt frequency
from (
        select *, max(recent_purchase) over() +1 reference_date,
          date_diff(recent_purchase, first_purchase, MONTH)+1 month_cnt
        from tb1
      )

--------------------------------------------------------------------------------

/*
OUTPUT TABLE 4 -----------------------------------------------------------------
*/

/*
Will save this data as a table by name of "rfm" for further use.
*/

/*
Guiding you through quintiles:
  - Grouping customers into five segments based on RFM scores for better insights.

  - Leveraging BigQuery's efficient APPROX_QUANTILES() for automatic calculations.

Quintile Insight :
  - Quintiles divide data into fifths, offering a balanced perspective.

APPROX_QUANTILES() Quick Take :
  - This function efficiently determines boundaries for RFM values.

In Practice :
  - In BigQuery, I applied APPROX_QUANTILES() for recency, frequency, and monetary values.

Note: Approximate functions ensure quick results while maintaining scalability.
*/

--------------------------------------------------------------------------------

select a.*,
       b.percentile_m [offset(20)] m_20,
       b.percentile_m [offset(40)] m_40,
       b.percentile_m [offset(60)] m_60,
       b.percentile_m [offset(80)] m_80,
       b.percentile_m [offset(100)] m_100,
       c.percentile_r [offset(20)] r_20,
       c.percentile_r [offset(40)] r_40,
       c.percentile_r [offset(60)] r_60,
       c.percentile_r [offset(80)] r_80,
       c.percentile_r [offset(100)] r_100,
       d.percentile_f [offset(20)] f_20,
       d.percentile_f [offset(40)] f_40,
       d.percentile_f [offset(60)] f_60,
       d.percentile_f [offset(80)] f_80,
       d.percentile_f [offset(100)] f_100
from `customer-segmentation-405712.retail.rfm` a,
      (
        select approx_quantiles(monetary, 100) percentile_m
        from `customer-segmentation-405712.retail.rfm`
      ) b,
      (
        select approx_quantiles(recency, 100) percentile_r
        from `customer-segmentation-405712.retail.rfm`
      ) c,
      (
        select approx_quantiles(frequency, 100) percentile_f
        from `customer-segmentation-405712.retail.rfm`
      ) d

--------------------------------------------------------------------------------

/*
Will save this data as a table by name of "quantile" for further use.
*/

/*
Assigning scores for RFM metrics is straightforward:

Comparison :
   - We evaluate each customer's RFM values in relation to others.

Scoring Logic :
   - Scores from 1 to 5 are assigned based on quintiles.
   - Higher scores for recency (R) signify more recent customers.
   - For frequency (F) and monetary value (M), higher quintiles mean higher scores.

Simplification :
   - Combine Frequency and Monetary values, reducing options from 125 to 50.

Implementation :
   - We use a CASE statement to fetch and assign scores.

Final Data :
   - Extract scores from the 'quintiles' table for further analysis.

This ensures a clear, standardized scoring system, reflecting each customer's
standing relative to others.
*/

--------------------------------------------------------------------------------

select CustomerID, r_score, recency, f_score, frequency, m_score, monetary,
  cast(round((f_score + m_score)/2) as int64) fm_score

from
(
  select *,
    case
      when monetary <= m_20 then 1
      when monetary <= m_40 and monetary > m_20 then 2
      when monetary <= m_60 and monetary > m_40 then 3
      when monetary <= m_80 and monetary > m_60 then 4
      when monetary <= m_100 and monetary > m_80 then 5
    end m_score,

    case
      when frequency <= f_20 then 1
      when frequency <= f_40 and frequency > f_20 then 2
      when frequency <= f_60 and frequency > f_40 then 3
      when frequency <= f_80 and frequency > f_60 then 4
      when frequency <= f_100 and frequency > f_80 then 5
    end f_score,

    case
      when recency <= r_20 then 5
      when recency <= r_40 and recency > r_20 then 4
      when recency <= r_60 and recency > r_40 then 3
      when recency <= r_80 and recency > r_60 then 2
      when recency <= r_100 and recency > r_80 then 1
    end r_score

  from `customer-segmentation-405712.retail.quantile`
) sub

--------------------------------------------------------------------------------

/*
OUTPUT TABLE 5 -----------------------------------------------------------------
*/

/*
Will save this data as a table by name of "score" for further use.
*/

/*
Explaining RFM segment definition:

Combining Scores :
   - Merge the calculated scores to assign each customer to a specific RFM segment.

Segment Definition :
   - With 5 groups for each R, F, and M metric, we have 125 potential combinations.

Guided by Personas :
   - Use the DMA's 11 personas as a reference to guide segment definitions based on R vs. FM scores.

Persona Examples :
  - Champions Segment :
     - Customers should have bought recently, bought often, and spent the most.
     - Therefore, their R score should be 5, and their combined FM score should be 4 or 5.

  - Can’t Lose Them Segment :
     - Customers made significant and frequent purchases but haven’t returned for a long time.
     - Their R score should be 1, and FM score should be 4 or 5.

This approach ensures a precise categorization of customers into RFM segments,
aligning with specific characteristics and behaviors.
*/

--------------------------------------------------------------------------------

SELECT CustomerID, r_score, f_score, m_score, fm_score,
  CASE
    WHEN (r_score = 5 AND fm_score = 5)
    OR (r_score = 5 AND fm_score = 4)
    OR (r_score = 4 AND fm_score = 5)
    THEN 'Champions'

    WHEN (r_score = 5 AND fm_score =3)
      OR (r_score = 4 AND fm_score = 4)
      OR (r_score = 3 AND fm_score = 5)
      OR (r_score = 3 AND fm_score = 4)
    THEN 'Loyal Customers'

    WHEN (r_score = 5 AND fm_score = 2)
      OR (r_score = 4 AND fm_score = 2)
      OR (r_score = 3 AND fm_score = 3)
      OR (r_score = 4 AND fm_score = 3)
    THEN 'Potential Loyalists'

    WHEN r_score = 5 AND fm_score = 1
    THEN 'Recent Customers'

    WHEN (r_score = 4 AND fm_score = 1)
      OR (r_score = 3 AND fm_score = 1)
    THEN 'Promising'

    WHEN (r_score = 3 AND fm_score = 2)
      OR (r_score = 2 AND fm_score = 3)
      OR (r_score = 2 AND fm_score = 2)
    THEN 'Customers Needing Attention'

    WHEN r_score = 2 AND fm_score = 1
    THEN 'About to Sleep'

    WHEN (r_score = 2 AND fm_score = 5)
      OR (r_score = 2 AND fm_score = 4)
      OR (r_score = 1 AND fm_score = 3)
    THEN 'At Risk'

    WHEN (r_score = 1 AND fm_score = 5)
      OR (r_score = 1 AND fm_score = 4)
    THEN 'Cant Lose Them'

    WHEN r_score = 1 AND fm_score = 2
    THEN 'Hibernating'

    WHEN r_score = 1 AND fm_score = 1
    THEN 'Lost'
  END AS rfm_segment
from `customer-segmentation-405712.retail.score`

--------------------------------------------------------------------------------

/*
OUTPUT TABLE 6 -----------------------------------------------------------------
*/

/*
After this step, each customer receives an RFM segment assignment,
emphasizing actual buying behavior. This segmentation method prioritizes buying
patterns, setting aside differences in motivations, intentions, and lifestyles.

While RFM simplifies the segmentation process, it's a powerful starting point.
Its simplicity allows for swift and automated execution, empowering companies
to make quick decisions and act on effective business strategies.
*/

/*
Certainly, let's incorporate the problem statement into the summary:

1. Project Objective :
   - Conducted an RFM analysis for a diverse retail store chain, specializing in
     various items and categories.

2. Business Challenge :
   - Addressed the challenge of optimizing marketing budgets and enhancing
     customer targeting for improved business impact.

3. Methodology :
   - Utilized recency, frequency, and monetary values to categorize customers,
     providing actionable insights into their buying behavior.

4. Segmentation Precision :
   - Defined RFM segments to focus marketing efforts on key customer groups,
     aligning with the business's strategic goals.

5. Simplicity and Speed :
   - Leveraged the simplicity of RFM analysis for swift and automated execution,
     enabling quick decision-making in marketing strategies.

6. Future Collaboration :
   - Appreciate your time in reviewing this project. I am eager to learn and
     contribute further, and open to collaboration on future projects. Your
     feedback is valuable. Thank you for the opportunity.
*/
