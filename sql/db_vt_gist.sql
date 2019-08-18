--https://www.postgresql.org/docs/9.4/functions-range.html



create table districts(
    id int not null,
    name varchar(50) not null,
    PRIMARY KEY(id)
);


create table patients(
    id serial not null,
    name varchar(100) not null,
    surname varchar(100) not null,
    id_district int not null,
    id_doctor int not null,
    enabled boolean DEFAULT TRUE,
    period tsrange not null CHECK (period <> '[,)'),
    EXCLUDE USING gist (id WITH =, period WITH &&),
    FOREIGN KEY (id_district) REFERENCES districts(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

/*
PK:
    insert
        gist: 150ms
    error
        gist: 18ms
*/
CREATE INDEX on patients USING HASH (id_doctor);




/*
Gist -> 20ms (failed)
Gist -> 16ms (correct)
*/
CREATE FUNCTION check_contigous_history_patients() RETURNS
TRIGGER AS 
DECLARE
    to_check int := NULL;
BEGIN 
   IF TG_OP = 'DELETE' THEN
        to_check = OLD.id;
   ELSE
        to_check = NEW.id;
   END IF;
   

   IF ( 
      EXISTS(
                SELECT id
                FROM patients p
                WHERE p.id = to_check AND (NOT '9999-01-01'::timestamp <@ period)
                      AND NOT EXISTS (
                                      SELECT p2.id 
                                      FROM patients p2 
                                      WHERE p2.id = p.id 
                                            AND p.period -|- p2.period
                                      )
            )       
  ) THEN RAISE
   EXCEPTION 'countigous_history_patients_violated';
END IF; RETURN NEW; END
LANGUAGE plpgsql
STABLE;

CREATE CONSTRAINT TRIGGER trigger_contigous_history_patients AFTER 
INSERT OR UPDATE OR DELETE ON patients
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_contigous_history_patients();



  




create table doctors(
    id serial not null,
    name varchar(100) not null,
    surname varchar(100) not null,
    id_district int not null,
    on_duty boolean DEFAULT TRUE,
    on_service boolean DEFAULT TRUE,
    period tsrange not null CHECK (period <> '[,)'),
    EXCLUDE USING gist (id WITH =, period WITH &&),
    FOREIGN KEY (id_district) REFERENCES districts(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);




CREATE FUNCTION check_contigous_history_doctors() RETURNS
TRIGGER AS 
DECLARE
    to_check int := NULL;
BEGIN 
   IF TG_OP = 'DELETE' THEN
        to_check = OLD.id;
   ELSE
        to_check = NEW.id;
   END IF;

   IF ( 
      EXISTS(
                SELECT id
                FROM doctors d
                WHERE d.id = to_check AND (NOT '9999-01-01'::timestamp <@ period)
                      AND NOT EXISTS (
                                      SELECT d2.id 
                                      FROM doctors d2 
                                      WHERE d2.id = d.id 
                                            AND d.period -|- d2.period
                                      )
            )         
  ) THEN RAISE
   EXCEPTION 'countigous_history_doctor_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;

CREATE CONSTRAINT TRIGGER trigger_contigous_history_doctors AFTER 
INSERT OR UPDATE OR DELETE ON doctors
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_contigous_history_doctors();


/*
We must ensure that the patients is referred, at any point of the time, 
to a on-duty doctor.
We have 2 differents approach:
    - Initially set end_date to Infinity for the doctor with on_duty
      true. When he will retire it will be set to false. But at this point
      the constraint will block the operation because there will be some
      patients related to this doctor from a certain start_date to their end_date
      (supposedly) set to Infinity (so valid at current time). So the application 
      requires first to move all patients to another doctor (than can be done
      automatically to the nearest doctor).
    - Recording a doctor we immediatly set the end_date where on_duty 
      will become false to the date of the (supposed) retirement pension.
      If the date changes we must update that end_date.
      Generally if it is anticipated we have the same problems as before.


Considering that we have guaranteed the contiguos history, here we have only 
to check border dates. 
We must also ensure that the doctor, in the period that is referenced
by the patient, is on-duty.
*/


/*
Gist (confirmed) -> 20ms
Gist (refused) -> 20ms
*/
CREATE FUNCTION check_fk_patients_to_doctors() RETURNS
TRIGGER AS 
DECLARE
    to_check int := NULL;
BEGIN 
    IF TG_OP = 'DELETE' THEN
        to_check = OLD.id;
    ELSE
        to_check = NEW.id;
    END IF;
    IF TG_TABLE_NAME = 'patients' THEN
        IF ( 
            EXISTS (SELECT p.id 
                      FROM patients p
                      WHERE p.id = to_check AND p.id_doctor IS NOT NULL AND
                            (NOT EXISTS (SELECT d.id
                                         FROM doctors d
                                         WHERE p.id_doctor = d.id AND
                                              lower(p.period) <@ d.period
                                        ) --When P starts exists such doctor
                             --When P ends exists such doctor is not necessary beacuse
                             --We have ensured contigous history, so if there is a valid row at the
                             --beginnig of the period, we are guarateed that there is a row also
                             --at the end. Now we have to ensure that this row has on_duty TRUE.
                            OR EXISTS     (SELECT d.id
                                           FROM doctors d
                                           WHERE p.id_doctor = d.id 
                                                 AND d.on_duty = FALSE AND
                                                 d.period && p.period
                                           )) --The doctor is always on-duty during patient's period
                      )  
        ) THEN RAISE EXCEPTION 'fk_patients_to_doctor_violated'; 
        END IF; 
    ELSE  --as before, but the id is referred to doctors
        IF ( 
            EXISTS (SELECT p.id 
                      FROM patients p
                      WHERE p.id_doctor = to_check AND
                            (NOT EXISTS (SELECT d.id
                                         FROM doctors d
                                         WHERE p.id_doctor = d.id AND
                                              lower(p.period) <@ d.period
                                        ) --When P starts exists such doctor
                            OR EXISTS     (SELECT d.id
                                           FROM doctors d
                                           WHERE p.id_doctor = d.id 
                                                 AND d.on_duty = FALSE AND
                                                 d.period && p.period
                                           )) --The doctor is always on-duty during patient's period
                      )  
        ) THEN RAISE EXCEPTION 'fk_doctors_to_patients_violated';
        END IF;
    END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;


CREATE CONSTRAINT TRIGGER trigger_fk_patients_to_doctors AFTER 
INSERT OR UPDATE ON patients
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_patients_to_doctors();

CREATE CONSTRAINT TRIGGER trigger_fk_patients_to_doctors AFTER 
UPDATE OR DELETE ON doctors
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_patients_to_doctors();



create table photos(
    id int not null,
    id_patient int not null,
    path text not null,
    --time timestamp not null DEFAULT NOW(),   only if we want to add history. In this case it must be part of PK
    PRIMARY KEY(id_patient,id) 
);


/*
Failure and success 15ms
*/
CREATE FUNCTION check_fk_photos_to_patients() RETURNS
TRIGGER AS 
BEGIN      
    IF TG_TABLE_NAME = 'photos' THEN
        IF ( 
            EXISTS (  SELECT p.id     --we simply check that every photo is linked to a patient
                      FROM patients p right join photos f ON p.id = f.id_patient
                      WHERE f.id = NEW.id AND p.id IS NULL
                     )        
        ) THEN RAISE EXCEPTION 'fk_photos_to_patients_violated';
        END IF; 
    ELSE
        IF ( 
            EXISTS (  SELECT p.id     --we simply check that every photo is linked to a patient
                      FROM patients p right join photos f ON p.id = f.id_patient
                      WHERE f.id_patient = OLD.id AND p.id IS NULL
                    )        
        ) THEN RAISE EXCEPTION 'fk_photos_to_patients_violated';
        END IF;
    END IF;
RETURN NEW; END
LANGUAGE plpgsql
STABLE;

CREATE CONSTRAINT TRIGGER trigger_fk_photos_to_patients AFTER 
INSERT OR UPDATE ON photos
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_photos_to_patients();

CREATE CONSTRAINT TRIGGER trigger_fk_patients_to_photos AFTER 
DELETE ON patients
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_photos_to_patients();


/*
To import a Dataset from .csv
150s
*/
COPY photos  FROM 'C:\xampp\htdocs\DBA\temporal\import\photos.csv' DELIMITERS ';' CSV;



create table prescriptions(
    id serial not null,
    id_patient int not null,
    id_doctor int not null,
    time timestamp not null DEFAULT NOW(),
    PRIMARY KEY(id)
);
CREATE INDEX idx_patient_on_prescription ON prescriptions USING HASH (id_patient);
CREATE INDEX idx_doctor_on_prescription ON prescriptions USING HASH (id_doctor);


/*
success 25ms
error  20ms
*/
CREATE FUNCTION check_fk_prescriptions_to_patients() RETURNS
TRIGGER AS BEGIN 
    IF TG_TABLE_NAME = 'prescriptions' THEN
           IF  ( 
                EXISTS (  --If the patient doesn't exists or the patient has a NULL value for id_doctor
                        SELECT p.id
                        FROM patients p right join prescriptions q ON p.id = q.id_patient
                        WHERE q.id = NEW.id AND (p.id IS NULL OR p.id_doctor IS NULL)
                       ) 
                OR 
                (
                    EXISTS (
                       SELECT p.id 
                       FROM patients p right join prescriptions q ON p.id = q.id_patient
                       WHERE q.id = NEW.id AND q.id_doctor NOT IN
                             (
                              SELECT id_doctor
                              FROM patients p2
                              WHERE p2.id = p.id AND 
                                    q.time >= lower(period)
                                    AND
                                    q.time < upper(period)
                                    AND id_doctor IS NOT NULL --to avoid issues with NOT IN applied on a NULL value that returns always false
                             )
                    )
                )
          ) THEN RAISE EXCEPTION 'fk_prescription_to_patients_violated';
          END IF; 
    ELSE
          IF  ( 
                EXISTS (  --If the patient doesn't exists or the patient has a NULL value for id_doctor
                        SELECT p.id
                        FROM patients p right join prescriptions q ON p.id = q.id_patient
                        WHERE (p.id IS NULL OR p.id_doctor IS NULL) --AND p.id = OLD.id   if we keep this statement, it doen't work. Beacuse if we deleted the single row of this id no tuples satify the condition, so the EXISTS will be never TRUE allowing us to delete the patients
                       ) 
                OR 
                (
                    EXISTS (
                       SELECT p.id 
                       FROM patients p right join prescriptions q ON p.id = q.id_patient
                       WHERE q.id_doctor NOT IN
                             (
                              SELECT id_doctor
                              FROM patients p2
                              WHERE p2.id = p.id AND 
                                    q.time >= lower(period)
                                    AND
                                    q.time < upper(period)
                                    AND id_doctor IS NOT NULL --to avoid issues with NOT IN applied on a NULL value that returns always false
                             )
                    )
                )
          ) THEN RAISE EXCEPTION 'fk_patients_to_prescriptions_violated';
          END IF; 
    END IF;
RETURN NEW; END
LANGUAGE plpgsql
STABLE;

/*
success 15ms
error (doctor not on duty) 14ms
*/
CREATE FUNCTION check_fk_prescriptions_to_doctors() RETURNS
TRIGGER AS BEGIN 
    IF TG_TABLE_NAME = 'prescriptions' THEN
            IF  ( 
                EXISTS (
                        SELECT * 
                        FROM doctors p right join prescriptions q ON p.id = q.id_doctor
                        WHERE q.id = NEW.id AND p.id IS NULL
                       ) 
                OR 
                (  --if the doctor isn't on_duty when the presciption is created
                    EXISTS (
                       SELECT * 
                       FROM doctors p right join prescriptions q ON p.id = q.id_doctor
                       WHERE q.id = NEW.id AND 
                             q.time >= lower(p.period)
                             AND
                             q.time < upper(p.period)
                             AND 
                             p.on_duty = FALSE
                       ) 
                )
            ) THEN RAISE EXCEPTION 'fk_prescription_to_doctors_violated';
            END IF; 
    ELSE
            IF  ( 
                EXISTS (
                        SELECT * 
                        FROM doctors p right join prescriptions q ON p.id = q.id_doctor
                        WHERE p.id IS NULL
                       ) 
                OR 
                (
                    EXISTS (
                       SELECT * 
                       FROM doctors p right join prescriptions q ON p.id = q.id_doctor
                       WHERE q.time >= lower(p.period)
                             AND
                             q.time < upper(p.period)
                             AND 
                             p.on_duty = FALSE
                       ) 
                )
            ) THEN RAISE EXCEPTION 'fk_doctors_to_prescriptions_violated';
            END IF; 
    END IF;
RETURN NEW; END
LANGUAGE plpgsql
STABLE;

CREATE CONSTRAINT TRIGGER trigger_fk_prescriptions_to_patients AFTER 
INSERT OR UPDATE ON prescriptions
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_prescriptions_to_patients();

CREATE CONSTRAINT TRIGGER trigger_fk_patients_to_prescriptions AFTER 
DELETE ON patients
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_prescriptions_to_patients();

CREATE CONSTRAINT TRIGGER trigger_fk_prescriptions_to_doctors AFTER 
INSERT OR UPDATE ON prescriptions
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_prescriptions_to_doctors();

CREATE CONSTRAINT TRIGGER trigger_fk_doctors_to_patients AFTER 
DELETE ON doctors
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_prescriptions_to_doctors();