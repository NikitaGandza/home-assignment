WITH CTE AS (
SELECT 	id,
		created ,
		lead_id ,
		algorithm ,
		country_code ,
		UNNEST(string_to_array(REPLACE(REPLACE (partner_id_ranking, '{', ''), '}', ''), ',')) AS partner_id_ranking ,
		no_valid_offers
FROM offer_listings
WHERE no_valid_offers = FALSE
)

SELECT * ,ROW_NUMBER() OVER(PARTITION BY id) AS resp_position
FROM CTE;
