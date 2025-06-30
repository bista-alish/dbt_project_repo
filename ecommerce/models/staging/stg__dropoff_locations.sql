SELECT 
    id,
    name,
    latitude,
    longitude 
FROM {{ ref('dropoff_locations') }}
