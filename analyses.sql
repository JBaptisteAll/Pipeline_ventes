-- ANALYSES DES VENTES - PME

-- a) Chiffre d'affaire total
SELECT
    ROUND(SUM(v.quantite * p.prix), 2) AS ca_total
FROM 
    ventes v
JOIN produits p 
    ON p.id_reference_produit = v.id_reference_produit
;

-- b) Ventes par produits
SELECT
    p.nom,
    SUM(v.quantite) AS total_quantite_vendue,
    ROUND(SUM(v.quantite * p.prix), 2) ca_produit
FROM 
    ventes v
JOIN produits p 
    ON p.id_reference_produit = v.id_reference_produit
GROUP BY 
    p.nom
ORDER BY 
    ca_produit desc
;

-- c) Chiffre d'affaire par région (ville)
SELECT
    m.ville AS region,
    SUM(v.quantite) AS total_quantite_vendue,
    ROUND(SUM(v.quantite * p.prix), 2) ca_region
FROM 
    ventes v
JOIN produits p 
    ON p.id_reference_produit = v.id_reference_produit
JOIN magasins m
    ON v.id_magasin = m.id_magasin
GROUP BY 
    m.ville
ORDER BY 
    ca_region desc
;