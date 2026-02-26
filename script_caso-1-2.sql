/*==========================================================
  Profesor: Eithel Klauss
  Alumno: Gustavo Dominguez

  EVALUACIÓN 3 - Hotel "La Última Oportunidad"
  Objetivo: Mejorar la gestión de cobranza y la emisión de informes.
  Script: Triggers + Package + Funciones + Procedimientos.
  Incluye integridad con triggers, manejo de errores y procesos paramétricos.
==========================================================*/

SET SERVEROUTPUT ON;

/*==========================================================
  CASO 1
  TRIGGER: Mantener TOTAL_CONSUMOS sincronizado con CONSUMO
  - Row level (FOR EACH ROW)
  - Uso de :OLD y :NEW
  - Soporta INSERT, UPDATE y DELETE
==========================================================*/

CREATE OR REPLACE TRIGGER TRG_ACT_TOTAL_CONSUMOS
AFTER INSERT OR UPDATE OR DELETE ON consumo
FOR EACH ROW
DECLARE
  v_delta NUMBER;
BEGIN
    
  -- Se inserta un nuevo consumo: Se suma al total
  IF INSERTING THEN
    
    UPDATE total_consumos
       SET monto_consumos = monto_consumos + :NEW.monto
     WHERE id_huesped = :NEW.id_huesped;
    
    -- Para crear el nuevo registro en caso de que aun no exista
    IF SQL%ROWCOUNT = 0 THEN
      INSERT INTO total_consumos (id_huesped, monto_consumos)
      VALUES (:NEW.id_huesped, :NEW.monto);
    END IF;

  -- Se eliminina un consumo: Rebajar el monto total
  ELSIF DELETING THEN  

    UPDATE total_consumos
       -- Se uso GREATEST por seguridad, dado que si el resultado da negativo, en vez de guardar un número negativo, guarda 0.
       SET monto_consumos = GREATEST(monto_consumos - :OLD.monto, 0)
     WHERE id_huesped = :OLD.id_huesped;

  -- Se actualiza un consumo: rebajar o aumentar de acuerdo a la diferencia
  ELSIF UPDATING THEN

    v_delta := NVL(:NEW.monto,0) - NVL(:OLD.monto,0);

    UPDATE total_consumos
       SET monto_consumos = GREATEST(monto_consumos + v_delta, 0)
     WHERE id_huesped = :NEW.id_huesped;

  END IF;

EXCEPTION
  WHEN OTHERS THEN
    RAISE;
END;
/
SHOW ERRORS;


/*==========================================================
  PRUEBAS CASO 1 (bloque anónimo)
  - Insertar consumo nuevo (id siguiente al último) para:
      cliente 340006, reserva 1587, monto 150
  - Eliminar consumo id 11473
  - Actualizar consumo id 10688 a monto 95
==========================================================*/

DECLARE
  v_new_id NUMBER;
BEGIN
  SELECT NVL(MAX(id_consumo),0) + 1 INTO v_new_id FROM consumo;

  INSERT INTO consumo (id_consumo, id_reserva, id_huesped, monto)
  VALUES (v_new_id, 1587, 340006, 150);

  DELETE FROM consumo
   WHERE id_consumo = 11473;

  UPDATE consumo
     SET monto = 95
   WHERE id_consumo = 10688;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('Caso 1 OK. Nuevo consumo insertado con ID: ' || v_new_id);
END;
/
-- verificacion de actualizacion:
SELECT * FROM total_consumos WHERE id_huesped IN (340006,340008);

/*==========================================================
  CASO 2
  PACKAGE: Función para total tours en USD + variable pública opcional
==========================================================*/

CREATE OR REPLACE PACKAGE PKG_COBRANZA_HOTEL IS
  -- variable pública (optativa) para exponer lo que devuelve la función
  g_monto_tours_usd NUMBER;

  -- función: monto total de tours en USD para un huésped
  FUNCTION FN_MONTO_TOURS_USD(p_id_huesped NUMBER) RETURN NUMBER;
END PKG_COBRANZA_HOTEL;
/

CREATE OR REPLACE PACKAGE BODY PKG_COBRANZA_HOTEL IS
  FUNCTION FN_MONTO_TOURS_USD(p_id_huesped NUMBER) RETURN NUMBER IS
    v_total NUMBER;
  BEGIN
    SELECT NVL(SUM(t.valor_tour * NVL(ht.num_personas,1)), 0)
      INTO v_total
      FROM huesped_tour ht
      JOIN tour t ON t.id_tour = ht.id_tour
     WHERE ht.id_huesped = p_id_huesped;

    g_monto_tours_usd := v_total;
    RETURN v_total;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      g_monto_tours_usd := 0;
      RETURN 0;
    WHEN OTHERS THEN
      g_monto_tours_usd := 0;
      RETURN 0;
  END FN_MONTO_TOURS_USD;
END PKG_COBRANZA_HOTEL;
/
SHOW ERRORS;


/*==========================================================
  FUNCION 1: Obtener agencia del huésped
  - Si hay error (incluye NO_DATA_FOUND) registra en REG_ERRORES con SQ_ERROR
  - Retorna "NO REGISTRA AGENCIA"
==========================================================*/

CREATE OR REPLACE FUNCTION FN_OBT_AGENCIA_HUESPED(p_id_huesped NUMBER)
RETURN VARCHAR2
IS
  v_agencia  VARCHAR2(40);
  v_id_error NUMBER;
  v_msg      VARCHAR2(400);
BEGIN

  SELECT a.nom_agencia
    INTO v_agencia
    FROM huesped h
    JOIN agencia a ON a.id_agencia = h.id_agencia
   WHERE h.id_huesped = p_id_huesped;

  RETURN v_agencia;

EXCEPTION
  WHEN OTHERS THEN

    v_msg := SQLERRM;
    SELECT sq_error.NEXTVAL INTO v_id_error FROM dual;

    INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
    VALUES (
      v_id_error,
      'Error en la función FN_OBT_AGENCIA_HUESPED al recuperar la agencia del huesped con Id '
      || TO_CHAR(p_id_huesped),
      v_msg
    );

    RETURN 'NO REGISTRA AGENCIA';
END;
/
SHOW ERRORS;


/*==========================================================
  FUNCION 2: Obtener consumos del huésped en USD (desde TOTAL_CONSUMOS)
  - Si no registra consumos, devuelve 0
==========================================================*/

CREATE OR REPLACE FUNCTION FN_OBT_CONSUMOS_USD(p_id_huesped NUMBER)
RETURN NUMBER
IS
  v_consumos NUMBER;
  v_id_error NUMBER;
  v_msg      VARCHAR2(400);
BEGIN

  SELECT monto_consumos
    INTO v_consumos
    FROM total_consumos
   WHERE id_huesped = p_id_huesped;

  RETURN NVL(v_consumos, 0);

EXCEPTION
  WHEN OTHERS THEN

    v_msg := SQLERRM;
    SELECT sq_error.NEXTVAL INTO v_id_error FROM dual;

    INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
    VALUES (
      v_id_error,
      'Error en la función FN_OBT_CONSUMOS_USD al recuperar los consumos del huesped con Id '
      || TO_CHAR(p_id_huesped),
      v_msg
    );

    RETURN 0;
END;
/
SHOW ERRORS;


/*==========================================================
  SUBPROGRAMA EXTRA (para que el proceso sea más limpio):
  Obtener % descuento por tramos de consumos
  - Si no encuentra tramo, 0%
==========================================================*/

CREATE OR REPLACE FUNCTION FN_OBT_PCT_DESC_CONSUMOS(p_consumos_usd NUMBER)
RETURN NUMBER
IS
  v_pct NUMBER;
BEGIN
  SELECT pct
    INTO v_pct
    FROM tramos_consumos
   WHERE p_consumos_usd BETWEEN vmin_tramo AND vmax_tramo;

  RETURN NVL(v_pct, 0);

EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
  WHEN OTHERS THEN
    RETURN 0;
END;
/
SHOW ERRORS;


/*==========================================================
  PROCEDIMIENTO PRINCIPAL: Generar DETALLE_DIARIO_HUESPEDES
  Parámetros:
   - p_fecha_actual: día a procesar
   - p_tipo_cambio: valor dólar
==========================================================*/

CREATE OR REPLACE PROCEDURE SP_GENERA_DETALLE_DIARIO(
  p_fecha_actual IN DATE,
  p_tipo_cambio  IN NUMBER
)
IS
  CURSOR c_salidas IS
    SELECT r.id_reserva,
           r.id_huesped,
           r.ingreso,
           r.estadia
      FROM reserva r
     WHERE TRUNC(r.ingreso + r.estadia) = TRUNC(p_fecha_actual);

  v_nombre_huesped      VARCHAR2(60);
  v_agencia             VARCHAR2(40);

  v_diario_habitacion   NUMBER;
  v_alojamiento_usd     NUMBER;
  v_consumos_usd        NUMBER;
  v_tours_usd           NUMBER;

  v_valor_personas_usd  NUMBER;

  v_subtotal_usd        NUMBER;

  v_pct_desc_consumos   NUMBER;
  v_desc_consumos_usd   NUMBER;

  v_desc_agencia_usd    NUMBER;
  v_total_usd           NUMBER;

  -- Valores en CLP (redondeados)
  v_alojamiento_clp     NUMBER;
  v_consumos_clp        NUMBER;
  v_tours_clp           NUMBER;
  v_subtotal_clp        NUMBER;
  v_desc_consumos_clp   NUMBER;
  v_desc_agencia_clp    NUMBER;
  v_total_clp           NUMBER;

BEGIN
  -- Limpieza para poder ejecutar el proceso cuantas veces se necesite
  EXECUTE IMMEDIATE 'TRUNCATE TABLE detalle_diario_huespedes';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE reg_errores';

  FOR reg IN c_salidas LOOP

    -- Nombre del huésped
    SELECT INITCAP(appat_huesped || ' ' || apmat_huesped || ' ' || nom_huesped)
      INTO v_nombre_huesped
      FROM huesped
     WHERE id_huesped = reg.id_huesped;

    -- Agencia 
    v_agencia := FN_OBT_AGENCIA_HUESPED(reg.id_huesped);

    -- Alojamiento: (habitación + minibar) diario * estadía
    -- Si hay más de una habitación asociada a la reserva, se suman los diarios.
    SELECT NVL(SUM(h.valor_habitacion + h.valor_minibar), 0)
      INTO v_diario_habitacion
      FROM detalle_reserva dr
      JOIN habitacion h ON h.id_habitacion = dr.id_habitacion
     WHERE dr.id_reserva = reg.id_reserva;

    v_alojamiento_usd := v_diario_habitacion * reg.estadia;

    -- Consumos desde TOTAL_CONSUMOS
    v_consumos_usd := FN_OBT_CONSUMOS_USD(reg.id_huesped);

    -- Tours desde Package
    v_tours_usd := PKG_COBRANZA_HOTEL.FN_MONTO_TOURS_USD(reg.id_huesped);

    /*
      Valor por persona:
      Se asume 1 persona por huésped por ausencia de numero de personas en reserva.
      Conversion de 35.000 CLP a USD para asegurar el cambio
    */
    v_valor_personas_usd := 35000 / p_tipo_cambio;

    -- Subtotal general de servicios (se incluye tours como parte del cobro total del huésped)
    v_subtotal_usd := v_alojamiento_usd
                    + v_consumos_usd
                    + v_valor_personas_usd
                    + v_tours_usd;

    -- Descuento por consumos (usamos TRAMOS_CONSUMOS como lógica de descuento)
    v_pct_desc_consumos := FN_OBT_PCT_DESC_CONSUMOS(v_consumos_usd);
    v_desc_consumos_usd := v_consumos_usd * v_pct_desc_consumos;

    -- Descuento agencia: solo si es VIAJES ALBERTI (12%)
    IF UPPER(v_agencia) = 'VIAJES ALBERTI' THEN
      -- Descuento adicional sobre el monto acumulado (después de descontar consumos)
      v_desc_agencia_usd := (v_subtotal_usd - v_desc_consumos_usd) * 0.12;
    ELSE
      v_desc_agencia_usd := 0;
    END IF;

    -- Total final (descuentos sobre base, tours se cobra completo)
    v_total_usd := v_subtotal_usd
               - v_desc_consumos_usd
               - v_desc_agencia_usd;

    -- Convertimos a CLP y redondeamos a enteros
    v_alojamiento_clp   := ROUND(v_alojamiento_usd * p_tipo_cambio);
    v_consumos_clp      := ROUND(v_consumos_usd * p_tipo_cambio);
    v_tours_clp         := ROUND(v_tours_usd * p_tipo_cambio);
    v_subtotal_clp      := ROUND(v_subtotal_usd * p_tipo_cambio);
    v_desc_consumos_clp := ROUND(v_desc_consumos_usd * p_tipo_cambio);
    v_desc_agencia_clp  := ROUND(v_desc_agencia_usd * p_tipo_cambio);
    v_total_clp         := ROUND(v_total_usd * p_tipo_cambio);

    INSERT INTO detalle_diario_huespedes (
      id_huesped,
      nombre,
      agencia,
      alojamiento,
      consumos,
      tours,
      subtotal_pago,
      descuento_consumos,
      descuentos_agencia,
      total
    ) VALUES (
      reg.id_huesped,
      v_nombre_huesped,
      v_agencia,
      v_alojamiento_clp,
      v_consumos_clp,
      v_tours_clp,
      v_subtotal_clp,
      v_desc_consumos_clp,
      v_desc_agencia_clp,
      v_total_clp
    );

  END LOOP;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('Proceso OK. DETALLE_DIARIO_HUESPEDES generado para: ' || TO_CHAR(p_fecha_actual, 'DD/MM/YYYY'));

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('Error en SP_GENERA_DETALLE_DIARIO: ' || SQLERRM);
    RAISE;
END;
/
SHOW ERRORS;


/*==========================================================
  PRUEBA CASO 2
  - "día actual" para el proceso: 18/08/2021
  - tipo de cambio: 915
==========================================================*/

BEGIN
  SP_GENERA_DETALLE_DIARIO(TO_DATE('18/08/2021','DD/MM/YYYY'), 915);
END;
/

-- Ver resultados
SELECT *
  FROM detalle_diario_huespedes
 ORDER BY agencia, nombre;

-- Ver errores capturados por FN_OBT_AGENCIA_HUESPED (si aplica)
SELECT *
  FROM reg_errores
 ORDER BY id_error;