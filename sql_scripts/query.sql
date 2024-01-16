with base as (
SELECT ld."loan_id", 
	bd."Borrower_Id" AS borrower_id,  
	to_date(ld."Date_of_release", 'MM-DD-YYYY') AS loan_date_of_release,
	ld."Term" AS term,
	ld."LoanAmount" AS loan_amount,
	ld."Downpayment" AS down_payment,
	bd."State" AS state,
	bd."City" AS city,
	bd."zip code" AS zip_code,
	ld."Payment_frequency" as payment_frequency,
	ld."Maturity_date" AS maturity_date, -- Throws an out of range for value "02/29/2023"
	to_date(sd."Expected_payment_date", 'MM-DD-YYYY') AS due_date,
	bd."borrower_credit_score",
	to_date(rd."Amount_paid", 'MM-DD-YYYY') AS date_paid,
	rd."Date_paid" AS amount_paid,
	sd."Expected_payment_amount" AS expected_payment_amount
	
	
FROM public."Borrower_Data" bd
LEFT JOIN PUBLIC."Loan_Data" ld ON bd."Borrower_Id" = ld."Borrower_id"
LEFT JOIN PUBLIC."Repayment_Data" rd ON ld."loan_id" = rd."loan_id(fk)"
LEFT JOIN PUBLIC."Schedule_Data" sd ON ld."loan_id" = sd."loan_id"
),

amount_at_risk_cte as (
SELECT DISTINCT loan_date_of_release, loan_id, expected_payment_amount
	FROM base
),

amount_at_risk_2 as (
SELECT SUM(expected_payment_amount) amount_at_risk, loan_id, loan_date_of_release
	FROM amount_at_risk_cte
	GROUP BY loan_date_of_release, loan_id
),

final_cte as (
SELECT *,	date_paid - due_date AS current_days_past_due,
	max(due_date) over(PARTITION BY loan_id) AS last_due_date,
	max(date_paid) over(PARTITION BY loan_id) AS last_repayment_date
FROM base
)

SELECT fc.loan_id,
		borrower_id,
		fc.loan_date_of_release,
		term,
		loan_amount,
		down_payment,
		state,
		city,
		zip_code,
		round(payment_frequency::numeric, 2) payment_frequency,
		maturity_date,
		current_days_past_due,
		last_due_date,
		last_repayment_date,
		round(sum(case when (current_days_past_due) > 0 then amar.amount_at_risk::numeric else 0 end), 2) amount_at_risk,
		borrower_credit_score,
		round(sum(amount_paid)::numeric, 2) total_amount_paid,
		round(sum(expected_payment_amount)::numeric, 2) total_amount_expected
		
FROM final_cte fc
left join amount_at_risk_2 amar using(loan_id)
group by fc.loan_id, borrower_id, fc.loan_date_of_release, term, loan_amount, down_payment, state, city, zip_code, payment_frequency,
		maturity_date, current_days_past_due, last_due_date, last_repayment_date, borrower_credit_score