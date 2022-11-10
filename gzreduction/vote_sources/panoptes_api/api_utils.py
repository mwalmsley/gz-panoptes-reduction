import json

# TODO could add to the client itself, I suppose
def login(Panoptes, login_loc):
    with open(login_loc, 'r') as f:
        zooniverse_login = json.load(f)
    Panoptes.connect(**zooniverse_login)  # modifies inplace