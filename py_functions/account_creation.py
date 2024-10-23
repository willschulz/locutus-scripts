import os
from mastodon import Mastodon
import requests
import pandas as pd
import mysql.connector

def get_db_connection():
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_DATABASE")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    
    # Establish the connection only when needed
    dbconn = mysql.connector.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database
    )
    
    return dbconn

def upload_avatar(aid, avatar):
    dbconn = get_db_connection()
    this_aid = aid
    this_avatar = avatar
    these_login_credentials = pd.read_sql_query("SELECT token, instance_base_url FROM mirror_accounts WHERE aid = '" + str(this_aid) + "'", dbconn).iloc[0]
    # only proceed if this_avatar is not NA or Null
    if this_avatar:
        #determine whether avatar is a CDN url based on the presence of 'cdn' in the url, otherwise assume it's a local file
        if 'cdn' in this_avatar:
            # if it is a CDN url, download the avatar
            avatar = requests.get(this_avatar)
            #determine the mime type
            mime_type = avatar.headers['Content-Type']
            # upload the avatar
            result = Mastodon(access_token = these_login_credentials['token'], api_base_url = these_login_credentials['instance_base_url']).account_update_credentials(avatar = avatar.content, avatar_mime_type=mime_type)
        else:  
            # if it is a local file, upload the avatar
            result = Mastodon(access_token = these_login_credentials['token'], api_base_url = these_login_credentials['instance_base_url']).account_update_credentials(avatar = this_avatar)
        #check the result to determine whether successful or not, save as boolean
    else:
        result = False
    dbconn.close()
    return result


# Goal: be able to do account creation in pure python too

# DigitalOcean stuff -- don't delete, but maybe not completely useful right now

# import os
# from pydo import Client

# client = Client(token=os.getenv("DO_PAT"))

# client.get(droplet_id = "451074271")
# client.get(droplet_id = "451074271").status

# #ssh_keys_resp = client.ssh_keys.list()
# #ssh_keys_resp

# client.droplets.list() #good
# this_droplet = client.droplet_actions.list(droplet_id = "451074271") #good
# this_droplet['actions'][0]['id']


#####################


import paramiko
import os

def run_ssh_command(command, hostname, port = 22, username='root', private_key_path='~/.ssh/id_mice_instances'):
    # Expand the tilde (~) in the private_key_path
    private_key_path = os.path.expanduser(private_key_path)

    # Create an SSH client instance
    ssh = paramiko.SSHClient()
    
    # Load host keys and set policy to automatically add missing keys
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Connect to the remote machine
        ssh.connect(hostname=hostname, port=port, username=username, key_filename=private_key_path)
        
        # Execute the command
        stdin, stdout, stderr = ssh.exec_command(command)
        
        # Read command output
        output = stdout.read().decode()
        errors = stderr.read().decode()
        
        # Print or return the output as needed
        print(f"Output:\n{output}")
        return output
        if errors:
            print(f"Errors:\n{errors}")

    finally:
        # Close the connection
        ssh.close()



# Example usage
hostname = 'remote_host_ip_or_domain'


dbconn = get_db_connection()

#todo: decide the best way of getting the hostname
hostname = pd.read_sql_query("SELECT * FROM mirror_verses WHERE droplet_id = '451074271'", dbconn).iloc[0]['instance_base_url'].replace("https://", "")
hostname

# Using the default private_key_path value
test = run_ssh_command(command='ls', hostname = hostname)

print(test)

# make python version of the following R functions:
# exec_cli_root <- function(droplet, cmd){
#     output <- capture.output(droplet_ssh(droplet = droplet, paste0("cd /var/www/mastodon/ && ", cmd), user = "root", keyfile = ssh_key_path))
#     return(output)
#   }
#   exec_tootctl <- function(droplet, cmd){
#     output <- capture.output(droplet_ssh(droplet = droplet, paste0("cd /var/www/mastodon/ && sudo -u mastodon RAILS_ENV=production bundle exec bin/tootctl ", cmd), user = "root", keyfile = ssh_key_path))
#     return(output)
#   }

# exec_cli_root <- function(hostname, cmd){
#     output <- capture.output(run_ssh_command(command = paste0("cd /var/www/mastodon/ && ", cmd), hostname = hostname))
#     return(output)
#   }

# exec_tootctl <- function(hostname, cmd){
#     output <- capture.output(run_ssh_command(command = paste0("cd /var/www/mastodon/ && sudo -u mastodon RAILS_ENV=production bundle exec bin/tootctl ", cmd), hostname = hostname))
#     return(output)
# }

# to create: aid, password, token, id, email
# still to create: token, id

#python version of exec_tootctl
def exec_tootctl(cmd, hostname, port = 22, username='root', private_key_path='~/.ssh/id_mice_instances'):
    command = f"cd /var/www/mastodon/ && sudo -u mastodon RAILS_ENV=production bundle exec bin/tootctl {cmd}"
    return run_ssh_command(command, hostname, port, username, private_key_path)



#testing values:
name="sparky3"
subdomain="test241011f"
domain = "argyle.social"
type=None
wid=None
script_user_id=None
clone_user_id=None
persona=None
ssh_key_path = "~/.ssh/id_mice_instances"
gmail_prefix = "argyle.systems"

# working! double check it does everything it needs to
def create_account(name, subdomain, domain = argyle.social, type=None, wid=None, script_user_id=None, clone_user_id=None, persona=None, ssh_key_path = "~/.ssh/id_mice_instances", gmail_prefix = "argyle.systems"):
    # prepare the command by pasting together the parts
    #R version: aid = paste0(names, ".", subdomain, ".", domain)
    instance_base_url = f"https://{subdomain}.{domain}"
    aid = f"{name}.{subdomain}.{domain}"
    #R version: email = paste0(gmail_prefix, "+", name, "@gmail.com")
    email = f"{gmail_prefix}+{name}@gmail.com"
    #R version: this_command = f"accounts create {name} --email {email} --confirmed --reattach --force"
    output = exec_tootctl(cmd = f"accounts create {name} --email {email} --confirmed --reattach --force", hostname = f"{subdomain}.{domain}")
    #R version: logins$password[i] <- str_replace(str_replace(output[str_detect(output, "New password")], pattern = "New password: ", ""), "\r", "")
    # python code to remove "New password: " and trailing "\n" from output
    password = output[output.find("New password: ") + len("New password: "):].strip()
    #approve all accounts
    exec_tootctl(cmd = "accounts approve --all", hostname = f"{subdomain}.{domain}")
    #user message that account has been created
    print(f"Account {name} has been created")
    #user message that we will now start credentialling accounts
    print("Credentialling Accounts")
    from mastodon import Mastodon
    #get the credentials
    master_credentials = pd.read_sql_query(f"SELECT master_app_client_id, master_app_client_secret FROM mirror_verses WHERE instance_base_url = '{instance_base_url}'", dbconn).iloc[0]
    #initialize the mastodon object
    mastodon = Mastodon(client_id = master_credentials['master_app_client_id'], client_secret = master_credentials['master_app_client_secret'], api_base_url = instance_base_url)
    #get token
    token = mastodon.log_in(email, password)
    #get mastodon id
    id = str(Mastodon(access_token = token, api_base_url = instance_base_url).me()['id'])
    #append the account to the mirror_accounts table
    accounts_toupload = pd.DataFrame([{"aid": aid, "name": name, "email": email, "password": password, "token": token, "instance_base_url": f"https://{subdomain}.{domain}", "type": type, "wid": wid, "id": id, "script_user_id": script_user_id, "clone_user_id": clone_user_id, "persona": persona}])
    # Write table to MySQL database
    dbconn = get_db_connection()
    cursor = dbconn.cursor()
    insert_query = """
        INSERT INTO mirror_accounts (aid, name, email, password, token, instance_base_url, type, wid, id, script_user_id, clone_user_id, persona)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = tuple(accounts_toupload.iloc[0])
    cursor.execute(insert_query, values)
    dbconn.commit()
    cursor.close()

    #accounts_toupload.to_sql("mirror_accounts", dbconn, if_exists = "append", index = False)
    #user message that the account has been saved
    print("Account has been saved")
    dbconn.close()
    #return the accounts_toupload, as an indicator that the process has succeeeded
    return accounts_toupload



# R version of remaining stuff to do in the above function:
# message("Credentialling Accounts")
# use_condaenv("locutus")
# py_config()
# py_run_string("import os")
# py_run_string("from mastodon import Mastodon")

# master_credentials <- fetch_table("mirror_verses") %>% filter(droplet_id == droplet$id) %>% select(master_app_client_id, master_app_client_secret) %>% unlist()
# master_credentials

# instance_base_url <- fetch_table("mirror_verses") %>% filter(droplet_id == droplet$id) %>% select(instance_base_url) %>% unlist()
# instance_base_url

# py_run_string("mastodon = Mastodon(client_id = r.master_credentials[0], client_secret = r.master_credentials[1], api_base_url = r.instance_base_url)") # need to find instance base url

# #py$instance_base_url = paste0("https://", r.subdomain, ".", r.domain)

# row = logins[1,]

# py_run_string("testcred = mastodon.log_in(r.row['email'], r.row['password'])")


# py_run_string("r.logins[0, 'token'] = testcred")

# for (i in 1:nrow(logins)){
#   email = logins$email[i]
#   password = logins$password[i]
#   py_run_string("token = mastodon.log_in(r.email, r.password)")
#   logins$token[i] <- py$token
# }


# message("Saving Mastodon IDs")
# mastodon_ids_char <- py_eval("[str(Mastodon(access_token = row['token'], api_base_url = row['instance_base_url']).me()['id']) for _, row in r.logins.iterrows()]")
# logins <- logins %>% mutate(id = mastodon_ids_char)

# #now save to mirror_accounts table of database
# accounts_toupload <- logins %>% transmute(aid, name, email, password, token, instance_base_url, type, wid = NA_character_, id, script_user_id, clone_user_id, persona)
# accounts_toupload$type[which(is.na(accounts_toupload$type))] <- "bot"

# # Write table to MySQL database
# message("Writing to MySQL")
# dbWriteTable(pool, "mirror_accounts", accounts_toupload, append = TRUE)
# fetch_table("mirror_accounts")