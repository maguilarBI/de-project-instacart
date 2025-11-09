# Instacart Data Engineering Project

## Contexto del Negocio
Instacart es una empresa de e-commerce que conecta usuarios con supermercados locales, permitiendo hacer pedidos de abarrotes con entrega a domicilio.

## Desafío
Optimización de la predicción de recompra mediante automatización de datos.

## Objetivo del Proyecto
Como equipo de Data Engineering, se genera un pipeline para abastecer de los datos necesarios al área de ML y puedan desarrollar el modelo.

## Pipeline Inicial (Entorno Develop)
Extracción de datos locales en CSV → Selección muestra aleatoria de 1% de los datos para acelerar el desarrollo en dev → Carga desde el entorno local a la nube (Azure)