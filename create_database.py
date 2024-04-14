import psycopg2
from psycopg2 import sql

def create_connection(dbname, user, password, host, port):
    """Create a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to database:", e)
        return None

def create_database(conn, dbname):
    """Create a database."""
    try:
        cur = conn.cursor()
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        conn.commit()
        cur.close()
        print("Database '{}' created successfully.".format(dbname))
        return True
    except psycopg2.Error as e:
        print("Error creating database:", e)
        return False

def create_role(conn, rolename, password=None):
    """Create a role."""
    try:
        cur = conn.cursor()
        if password:
            cur.execute(sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD %s").format(sql.Identifier(rolename)), (password,))
        else:
            cur.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(rolename)))
        conn.commit()
        cur.close()
        print("Role '{}' created successfully.".format(rolename))
        return True
    except psycopg2.Error as e:
        print("Error creating role:", e)
        return False

def grant_privileges(conn, rolename, dbname, privileges):
    """Grant privileges to a role."""
    try:
        cur = conn.cursor()
        for privilege in privileges:
            cur.execute(sql.SQL("GRANT {} ON DATABASE {} TO {}").format(sql.Identifier(privilege), sql.Identifier(dbname), sql.Identifier(rolename)))
        conn.commit()
        cur.close()
        print("Privileges granted to role '{}' on database '{}'.".format(rolename, dbname))
        return True
    except psycopg2.Error as e:
        print("Error granting privileges:", e)
        return False

def grant_role(conn, role_to_grant, role_to_grant_to):
    """Grant a role to another role."""
    try:
        cur = conn.cursor()
        cur.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier(role_to_grant), sql.Identifier(role_to_grant_to)))
        conn.commit()
        cur.close()
        print("Role '{}' granted to role '{}'.".format(role_to_grant, role_to_grant_to))
        return True
    except psycopg2.Error as e:
        print("Error granting role:", e)
        return False

def main():
    # Get input
    instance_name = input("Enter instance name: ")
    dbname = input("Enter database name: ")

    # Connect to PostgreSQL (you may need to adjust these parameters)
    conn = create_connection("postgres", "postgres", "password", "localhost", "5432")
    if conn is None:
        return

    # Check if database already exists
    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_database WHERE datname = %s", (dbname,))
    exists = cur.fetchone()
    cur.close()

    if exists:
        print("Database '{}' already exists.".format(dbname))
    else:
        # Create database
        if create_database(conn, dbname):
            # Create roles
            roles = [dbname + "_app", dbname + "_owner", dbname + "_reader"]
            for role in roles:
                if not create_role(conn, role):
                    # If role creation fails, rollback database creation
                    conn.rollback()
                    conn.close()
                    return

            # Grant privileges
            for role in [dbname + "_owner", dbname + "_reader"]:
                if not grant_privileges(conn, role, dbname, ["CONNECT"]):
                    # If granting privileges fails, rollback database creation
                    conn.rollback()
                    conn.close()
                    return

            # Grant all privileges to owner role
            if not grant_privileges(conn, dbname + "_owner", dbname, ["ALL"]):
                # If granting privileges fails, rollback database creation
                conn.rollback()
                conn.close()
                return

            # Grant role to user
            if not grant_role(conn, dbname + "_owner", dbname + "_app"):
                # If granting role fails, rollback database creation
                conn.rollback()
                conn.close()
                return

    conn.close()

if __name__ == "__main__":
    main()

