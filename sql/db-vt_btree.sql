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
    start_date date not null,
    end_date date not null,
    --PRIMARY KEY(id),    substituted with an assertion
    FOREIGN KEY (id_district) REFERENCES districts(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);


/*
Why It isn't possible to use simply PRIMARY KEY(id, end_date) ?
Well, consider the following rows :
    1. Mario Rossi valid in [1998-01-01, 2019-01-01) have id_doctor = 10
    2. Mario Rossi valid in [1998-01-01, 2018-01-01) have id_doctor = 65
The PRIMARY constraint is respected, but during [1998-01-01,2018-01-01) which 
doctor do we have to consider valid ?
We must ensure that the id of patients is unique at any point of the time.
It requires a sequenced primary key constraint, that can be expressed by the
following constraint:
*/

create index on patients (id,start_date,end_date);

/*
inserted:
    BTREE: 7s
error:
    BTREE: 600ms
*/
CREATE FUNCTION check_p_key_patients_v1() RETURNS
TRIGGER AS $$BEGIN 
   IF NOT ( 
      NOT EXISTS (SELECT * 
                  FROM patients p1
                  WHERE 1 < (SELECT COUNT(id)
                                 FROM patients p2
                                 WHERE p2.id = p1.id AND
                                       (p1.start_date, p1.end_date) OVERLAPS
                                       (p2.start_date, p2.end_date) )

                  )        
  ) THEN RAISE
   EXCEPTION 'primary_key_patients_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;

start transaction;
insert into patients VALUES (1,'Steve','Azzolin',15,1,TRUE,'1998-06-17','9999-01-01');
commit;


/*
inserted:
    BTREE: 10ms
error:
    BTREE: 12ms
*/
CREATE FUNCTION check_p_key_patients_v2() RETURNS
TRIGGER AS $$BEGIN 
   IF NOT ( 
      NOT EXISTS (SELECT * 
                  FROM patients p1
                  WHERE p1.id = NEW.id 
                        AND 1 < (SELECT COUNT(id)
                                 FROM patients p2
                                 WHERE p2.id = p1.id AND
                                       (p1.start_date, p1.end_date) OVERLAPS
                                       (p2.start_date, p2.end_date) )

                  )        
  ) THEN RAISE
   EXCEPTION 'primary_key_patients_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;

CREATE CONSTRAINT TRIGGER trigger_p_key_patients AFTER 
INSERT OR UPDATE ON patients
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_p_key_patients();


--Should fail
start transaction;
insert into patients VALUES (1,'Steve','Azzolin',15,1,TRUE,'1998-06-11','1998-06-12');
commit;

/*
inserted:
    BTREE: (server rebooted)
error:
    BTREE: (server rebooted)
*/
CREATE FUNCTION check_contigous_history_patients_v1() RETURNS
TRIGGER AS $$BEGIN 
   IF NOT ( 
      NOT EXISTS (SELECT * 
                  FROM patients p1 inner join patients p2 ON p1.id = p2.id
                  WHERE p1.end_date < p2.start_date
                        AND 
                        NOT EXISTS (SELECT *
                                    FROM patients p3
                                    WHERE p3.id = p1.id AND
                                         ((p3.start_date <= p1.end_date
                                           AND
                                          p3.end_date > p1.end_date) 
                                         OR
                                         (p3.start_date < p2.start_date
                                           AND
                                          p3.end_date >= p2.start_date)) 
                                    )
                  )        
  ) THEN RAISE
   EXCEPTION 'countigous_history_patients_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;

/*
inserted:
    BTREE: 15ms
error:
    BTREE: 13ms
*/
CREATE FUNCTION check_contigous_history_patients_v2() RETURNS
TRIGGER AS 
$$DECLARE
    to_check int := NULL;
BEGIN 
   IF TG_OP = 'DELETE' THEN
        to_check = OLD.id;
   ELSE
        to_check = NEW.id;
   END IF;

   IF NOT ( 
      NOT EXISTS (SELECT * 
                  FROM patients p1 inner join patients p2 ON p1.id = p2.id
                  WHERE p1.id = to_check AND p1.end_date < p2.start_date
                        AND 
                        NOT EXISTS (SELECT *
                                    FROM patients p3
                                    WHERE p3.id = p1.id AND
                                         ((p3.start_date <= p1.end_date
                                           AND
                                          p3.end_date > p1.end_date) 
                                         OR
                                         (p3.start_date < p2.start_date
                                           AND
                                          p3.end_date >= p2.start_date))  
                                    )
                  )        
  ) THEN RAISE
   EXCEPTION 'countigous_history_patients_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;


/*
inserted:
    BTREE: 10ms
error:
    BTREE: 20ms
*/
CREATE FUNCTION check_contigous_history_patients_v3() RETURNS
TRIGGER AS $$BEGIN 
   IF ( 
      EXISTS(
                SELECT id
                FROM patients d
                WHERE d.id = NEW.id AND d.end_date <> '9999-01-01'
                      AND NOT EXISTS (
                                      SELECT d2.id 
                                      FROM patients d2 
                                      WHERE d2.id = d.id 
                                            AND d2.start_date = d.end_date
                                      )
            )       
  ) THEN RAISE
   EXCEPTION 'countigous_history_doctor_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;

CREATE CONSTRAINT TRIGGER trigger_contigous_history_patients_v1 AFTER 
INSERT OR UPDATE OR DELETE ON patients
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_contigous_history_patients_v1();

  


create table doctors(
    id serial not null,
    name varchar(100) not null,
    surname varchar(100) not null,
    id_district int not null,
    on_duty boolean DEFAULT TRUE,
    on_service boolean DEFAULT TRUE,
    start_date date not null,
    end_date date not null, 
    FOREIGN KEY (id_district) REFERENCES districts(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

/*
Similar  to patients
*/
CREATE FUNCTION check_p_key_doctors() RETURNS
TRIGGER AS $$BEGIN 
   IF NOT ( 
      NOT EXISTS (SELECT * 
                  FROM doctors p1
                  WHERE 1 < (SELECT COUNT(id)
                                 FROM doctors p2
                                 WHERE p2.id = p1.id AND
                                       (p1.start_date, p1.end_date) OVERLAPS
                                       (p2.start_date, p2.end_date) )
                  )        
  ) THEN RAISE
   EXCEPTION 'primary_key_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;

CREATE CONSTRAINT TRIGGER trigger_p_key_doctors AFTER 
INSERT OR UPDATE ON doctors
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_p_key_doctors();


/*
Similar to patients
*/
CREATE FUNCTION check_contigous_history_doctors() RETURNS
TRIGGER AS $$BEGIN 
   IF NOT ( 
      NOT EXISTS (SELECT * 
                  FROM doctors p1 inner join doctors p2 ON p1.id = p2.id
                  WHERE p1.end_date < p2.start_date
                        AND 
                        NOT EXISTS (SELECT *
                                    FROM doctors p3
                                    WHERE p3.id = p1.id AND
                                         (p3.start_date <= p1.end_date
                                           AND
                                          p3.end_date > p1.end_date) 
                                         OR
                                         (p3.start_date < p2.start_date
                                           AND
                                          p3.end_date >= p2.start_date)  
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
inserted:
    BTREE: (pg reboot)
error:
    BTREE: 4s
*/
CREATE FUNCTION check_fk_patients_to_doctors_v1() RETURNS
TRIGGER AS $$BEGIN 
    IF NOT ( 
      NOT EXISTS (SELECT p.id 
                  FROM patients p
                  WHERE p.id_doctor IS NOT NULL AND
                        (NOT EXISTS (SELECT *
                                     FROM doctors d
                                     WHERE p.id_doctor = d.id AND
                                           d.start_date <= p.start_date
                                           AND
                                           p.start_date < d.end_date
                                    ) --When P starts exists such doctor
                        OR NOT EXISTS (SELECT *
                                       FROM doctors d
                                       WHERE p.id_doctor = d.id AND
                                             d.start_date < p.end_date
                                             AND
                                             p.end_date <= d.end_date
                                       ) --When P ends exists such doctor
                        OR EXISTS     (SELECT *
                                       FROM doctors d
                                       WHERE p.id_doctor = d.id 
                                             AND d.on_duty = FALSE AND
                                             d.start_date < p.end_date
                                             AND d.end_date > p.start_date
                                       )) --The doctor is always on-duty during patient's period
                  )        
   ) THEN RAISE EXCEPTION 'fk_patients_to_doctor_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;


/*
inserted:
    BTREE: 165ms
error:
    BTREE: 200ms
*/
CREATE FUNCTION check_fk_patients_to_doctors_v2() RETURNS
TRIGGER AS 
$$DECLARE
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
                                              d.start_date <= p.start_date
                                              AND
                                              p.start_date < d.end_date
                                        ) --When P starts exists such doctor
                             --'When P ends exists such doctor' is not necessary beacuse
                             --We have ensured contigous history, so if there is a valid row at the
                             --beginnig of the period, we are guarateed that there is a row also
                             --at the end. Now we have to ensure that this row has on_duty TRUE.
                            OR EXISTS     (SELECT d.id
                                           FROM doctors d
                                           WHERE p.id_doctor = d.id 
                                                 AND d.on_duty = FALSE AND
                                                 d.start_date < p.end_date
                                                 AND d.end_date > p.start_date
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
                                              d.start_date <= p.start_date
                                              AND
                                              p.start_date < d.end_date
                                        ) 
                            OR EXISTS     (SELECT d.id
                                           FROM doctors d
                                           WHERE p.id_doctor = d.id 
                                                 AND d.on_duty = FALSE AND
                                                 d.start_date < p.end_date
                                                 AND d.end_date > p.start_date
                                           )) 
                      )  
        ) THEN RAISE EXCEPTION 'fk_doctors_to_patients_violated';
        END IF;
    END IF;
RETURN NEW; END$$
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





/*
Photo is used to identify a patient in case of homonymy. It is not required
to add an history to photos, that could be simply implemented (in this case, legitimately)
as a series of photos with its own timestamps.
So this table records current data, thus the FK simply refers to a particular patients,
regardless of valid time of the tuple, but at any time that the patient is
in our system. 
*/
create table photos(
    id int not null,
    id_patient int not null,
    path text not null,
    --time timestamp not null DEFAULT NOW(),   only if we want to add history. In this case it must be part of PK
    PRIMARY KEY(id_patient,id)
);

/*
It is necessary to define such constraint because the FOREIGN KEY 
built-in constraint requires to be referenced to an Unique index.
Patients doesn't have this uniqie index, for reasons already explained.
*/


/*
inserted:
    BTREE: 346ms
error:
    BTREE: 16ms
*/
CREATE FUNCTION check_fk_photos_to_patients_v1() RETURNS
TRIGGER AS $$BEGIN 
   IF ( 
        EXISTS  ( SELECT p.id     --we simply check that every photo is linked to a patient
                  FROM patients p right join photos f ON p.id = f.id_patient
                  WHERE p.id IS NULL
                )        
  ) THEN RAISE
   EXCEPTION 'fk_photos_to_patients_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;

/*
inserted:
    BTREE: 11ms
error:
    BTREE: 11ms
*/
CREATE FUNCTION check_fk_photos_to_patients_v2() RETURNS
TRIGGER AS 
$$BEGIN      
    IF TG_TABLE_NAME = 'photos' THEN
        IF ( 
            EXISTS (  SELECT p.id     
                      FROM patients p right join photos f ON p.id = f.id_patient
                      WHERE f.id = NEW.id AND p.id IS NULL
                     )        
        ) THEN RAISE EXCEPTION 'fk_photos_to_patients_violated';
        END IF; 
    ELSE
        IF ( 
            EXISTS (  SELECT p.id     
                      FROM patients p right join photos f ON p.id = f.id_patient
                      WHERE f.id_patient = OLD.id AND p.id IS NULL
                    )        
        ) THEN RAISE EXCEPTION 'fk_photos_to_patients_violated';
        END IF;
    END IF;
RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;

CREATE CONSTRAINT TRIGGER trigger_fk_photos_to_patients_v1 AFTER 
INSERT OR UPDATE ON photos
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_photos_to_patients_v1();

CREATE CONSTRAINT TRIGGER trigger_fk_patients_to_photos_v1 AFTER 
DELETE ON patients
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_photos_to_patients_v1();

CREATE CONSTRAINT TRIGGER trigger_fk_photos_to_patients_v2 AFTER 
INSERT OR UPDATE ON photos
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_photos_to_patients_v2();

CREATE CONSTRAINT TRIGGER trigger_fk_patients_to_photos_v2 AFTER 
DELETE ON patients
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_photos_to_patients_v2();


/*
We must guarantee that at the time the prescription is made, the patient 
is currently referring the doctor that prescribes the prescption, and
that the doctor is actually on duty.
*/
create table prescriptions(
    id serial not null,
    id_patient int not null,
    id_doctor int not null,
    time timestamp not null DEFAULT NOW(),
    PRIMARY KEY(id)
);


/*
inserted:
    BTREE: 58s
error:
    BTREE: 400ms
*/
CREATE FUNCTION check_fk_prescriptions_to_patients_v1() RETURNS
TRIGGER AS $$BEGIN 
   IF  ( 
        EXISTS (  --If the patient doesn't exists or the patient has a NULL value for id_doctor
                SELECT p.id
                FROM patients p right join prescriptions q ON p.id = q.id_patient
                WHERE p.id IS NULL OR p.id_doctor IS NULL
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
                      WHERE p2.id = p.id
                            AND
                            q.time >= start_date
                            AND
                            q.time < end_date
                            AND id_doctor IS NOT NULL --to avoid issues with NOT IN applied on a NULL value that returns always false
                     )
            )
        )
  ) THEN RAISE
   EXCEPTION 'fk_prescription_to_patients_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;


/*
inserted:
    BTREE: 12ms
error:
    BTREE: 11ms
*/
CREATE FUNCTION check_fk_prescriptions_to_patients_v2() RETURNS
TRIGGER AS $$BEGIN 
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
                                    q.time >= p2.start_date
                                    AND
                                    q.time < p2.end_date
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
                                    q.time >= start_date
                                    AND
                                    q.time < end_date
                                    AND id_doctor IS NOT NULL --to avoid issues with NOT IN applied on a NULL value that returns always false
                             )
                    )
                )
          ) THEN RAISE EXCEPTION 'fk_patients_to_prescriptions_violated';
          END IF; 
    END IF;
RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;

CREATE CONSTRAINT TRIGGER trigger_fk_prescriptions_to_patients AFTER 
INSERT OR UPDATE ON prescriptions
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_prescriptions_to_patients_v1();

CREATE CONSTRAINT TRIGGER trigger_fk_patients_to_prescriptions AFTER 
UPDATE OR DELETE ON patients
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_prescriptions_to_patients_v1();


CREATE CONSTRAINT TRIGGER trigger_fk_prescriptions_to_patients AFTER 
INSERT OR UPDATE ON prescriptions
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_prescriptions_to_patients_v2();

CREATE CONSTRAINT TRIGGER trigger_fk_patients_to_prescriptions AFTER 
DELETE ON patients  --to include UPDATE we could use the same tecnique of check_fk_patients_to_doctors_v2 with the variable to_check
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_prescriptions_to_patients_v2();


/*
Similat to presc_to_patients
*/
CREATE FUNCTION check_fk_prescriptions_to_doctors() RETURNS
TRIGGER AS $$BEGIN 
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
               WHERE q.time >= p.start_date
                     AND
                     q.time < p.end_date
                     AND 
                     p.on_duty = FALSE
               ) 
        )
  ) THEN RAISE
   EXCEPTION 'fk_prescription_to_doctors_violated';
END IF; RETURN NEW; END$$
LANGUAGE plpgsql
STABLE;

CREATE CONSTRAINT TRIGGER trigger_fk_prescriptions_to_doctors AFTER 
INSERT OR UPDATE ON prescriptions
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_prescriptions_to_doctors();

CREATE CONSTRAINT TRIGGER trigger_fk_doctors_to_patients AFTER 
UPDATE OR DELETE ON doctors
DEFERRABLE INITIALLY DEFERRED 
FOR EACH ROW
EXECUTE PROCEDURE check_fk_prescriptions_to_doctors();




COPY photos  FROM '.\photos.csv' DELIMITERS ';' CSV;