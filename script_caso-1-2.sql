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
  PACKAGE PKG_COBRANZA_HOTEL

  Este package centraliza el cálculo del monto total en USD
  correspondiente a los tours contratados por un huésped.

  Se expone:
  - Una variable pública (opcional) para dejar disponible
    el último monto calculado.
  - Una función que obtiene el total de tours multiplicando
    el valor del tour por el número de personas asociadas.
==========================================================*/

CREATE OR REPLACE PACKAGE PKG_COBRANZA_HOTEL IS
  -- variable pública (optativa) para exponer lo que devuelve la función
  g_monto_tours_usd NUMBER;

  -- función: monto total de tours en USD para un huésped
  FUNCTION FN_MONTO_TOURS_USD(p_id_huesped NUMBER) RETURN NUMBER;
END PKG_COBRANZA_HOTEL;
/


CREATE OR REPLACE PACKAGE BODY PKG_COBRANZA_HOTEL IS
  /*  
    Calcula el total de tours contratados por un huésped.
    Si el huésped no registra tours, retorna 0.
  */
  FUNCTION FN_MONTO_TOURS_USD(p_id_huesped NUMBER) RETURN NUMBER IS
    v_total NUMBER;
  BEGIN
   --  Se utiliza NVL para asegurar que el proceso nunca interrumpa el flujo principal por ausencia de datos.
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
  FUNCION: FN_OBT_AGENCIA_HUESPED
  Recupera la agencia asociada al huésped.
  Si ocurre algún problema (datos inexistentes o error),
  se registra en REG_ERRORES y se retorna el texto
  'NO REGISTRA AGENCIA', tal como indican las reglas.
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
  FUNCION: FN_OBT_CONSUMOS_USD
  Obtiene el monto total de consumos del huésped
  desde la tabla TOTAL_CONSUMOS.

  Si el huésped no registra consumos o ocurre un error,
  el proceso no se detiene y retorna 0, registrando
  el incidente en REG_ERRORES.
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
  SUBPROGRAMA EXTRA FN_OBT_PCT_DESC_CONSUMOS (para que el proceso quede mas limpio y legible)
  Determina el porcentaje de descuento aplicable a los
  consumos, cruzando el monto con la tabla TRAMOS_CONSUMOS.

  Si el monto no cae en ningún tramo, se asume 0%.
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
  FUNCION: FN_OBT_TIPO_HABITACION
  Recupera el tipo de habitación asociado a una reserva.
  Este dato es necesario para determinar la capacidad
  máxima y calcular el recargo por persona.
  
  - Retorna tipo_habitacion
  - Si hay error, registra en REG_ERRORES
  - Retorna 'S' por defecto
==========================================================*/

CREATE OR REPLACE FUNCTION FN_OBT_TIPO_HABITACION(p_id_reserva NUMBER)
RETURN VARCHAR2
IS
  v_tipo_habitacion VARCHAR2(2);
  v_msg             VARCHAR2(400);
  v_id_error        NUMBER;
BEGIN

  SELECT h.tipo_habitacion
    INTO v_tipo_habitacion
    FROM detalle_reserva dr
    JOIN habitacion h ON h.id_habitacion = dr.id_habitacion
   WHERE dr.id_reserva = p_id_reserva
     AND ROWNUM = 1;

  RETURN v_tipo_habitacion;

EXCEPTION
  WHEN OTHERS THEN

    v_msg := SQLERRM;
    SELECT sq_error.NEXTVAL INTO v_id_error FROM dual;

    INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
    VALUES (
      v_id_error,
      'Error en FN_OBT_TIPO_HABITACION al recuperar el tipo de habitacion de la reserva con Id '
      || TO_CHAR(p_id_reserva),
      v_msg
    );

    RETURN 'S'; -- valor por defecto
END;
/
SHOW ERRORS;


/*==========================================================
  PROCEDIMIENTO PRINCIPAL: Generar DETALLE_DIARIO_HUESPEDES
  Genera el detalle diario de huéspedes que finalizan
  su estadía en la fecha indicada.

  El procedimiento:
  - Calcula alojamiento (habitación + minibar por días).
  - Aplica recargo por persona según capacidad.
  - Obtiene consumos y tours.
  - Determina descuentos por tramos y por agencia.
  - Calcula subtotal y total final.
  - Convierte los valores a CLP según tipo de cambio.
  - Registra errores sin interrumpir el proceso.

  Parámetros:
    p_fecha_actual : fecha a procesar
    p_tipo_cambio  : valor del dólar
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
  v_tipo_habitacion     VARCHAR2(2);
  v_capacidad_personas  NUMBER;
  v_personas_total      NUMBER;

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

    -- Se calcula el valor diario total (habitación + minibar)
    -- y luego se multiplica por la cantidad de días de estadía.
    SELECT NVL(SUM(h.valor_habitacion + h.valor_minibar), 0)
      INTO v_diario_habitacion
      FROM detalle_reserva dr
      JOIN habitacion h ON h.id_habitacion = dr.id_habitacion
     WHERE dr.id_reserva = reg.id_reserva;
     
    -- Se obtiene el tipo de habitacion
    v_tipo_habitacion := FN_OBT_TIPO_HABITACION(reg.id_reserva);
        
    -- Se determina capacidad según tipo de habitacion
    IF v_tipo_habitacion = 'S' THEN
       v_capacidad_personas := 1;
    ELSIF v_tipo_habitacion = 'D' THEN
       v_capacidad_personas := 2;
    ELSIF v_tipo_habitacion = 'T' THEN
       v_capacidad_personas := 3;
    ELSIF v_tipo_habitacion = 'C' THEN
       v_capacidad_personas := 4;
    ELSE
       v_capacidad_personas := 1;
    END IF;
    
    -- Personas por estadía completa
    v_personas_total := v_capacidad_personas * reg.estadia;
    
    -- Convertir 35.000 CLP a USD y multiplicar por personas
    v_valor_personas_usd := (35000 * v_personas_total) / p_tipo_cambio;
    
    -- Calculo de valor alojamiento en USD multiplicando el valor diario por el numero de dias
    v_alojamiento_usd := v_diario_habitacion * reg.estadia;

    -- Consumos desde TOTAL_CONSUMOS
    v_consumos_usd := FN_OBT_CONSUMOS_USD(reg.id_huesped);

    -- Tours desde Package
    v_tours_usd := PKG_COBRANZA_HOTEL.FN_MONTO_TOURS_USD(reg.id_huesped);

    -- El subtotal se compone estrictamente de:
    -- Alojamiento + Consumos + Valor por persona.
    -- Los tours se registran, pero no forman parte del subtotal.
    v_subtotal_usd := v_alojamiento_usd
                    + v_consumos_usd
                    + v_valor_personas_usd;

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

    -- El total final corresponde a:
    -- Subtotal - Descuento por consumos - Descuento por agencia (Si aplica).
    -- Los tours no participan en este cálculo.
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
