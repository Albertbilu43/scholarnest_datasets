# Databricks notebook source
spark.version

# COMMAND ----------


while True:
    password = input('Enter password: ')

    if password.lower() == 'quit':
        break

    is_long = len(password) >= 8
    has_upper= any(char.isupper() for char in password)
    has_lower= any(char.islower() for char in password)
    has_digit= any(char.isdigit() for char in password)
    has_special= any(not char.isalnum() for char in password)

    if is_long and has_upper and has_lower and has_digit and has_special:
        print('Password is strong')
        break # Exit the loop if password is valid

    else: 
        missing=[]
        if not is_long: missing.append('At least 8 characters')
        if not has_upper: missing.append('uppercase letter')
        if not has_lower: missing.append('lowercase letter')        
        if not has_digit: missing.append('numerical')
        if not has_special: missing.append('special character')

        print(f"Password is weak. Missing {', '.join(missing)}.")
        print('Please try again')






# COMMAND ----------

