""" Unit tests of process.py """
import pytest
import os
import pandas as pd
from process import process

@pytest.fixture(scope="session")
def temp_dir_json_files(tmp_path_factory):
    """Create a temp directory with 2 JSON files"""
    test_dir = tmp_path_factory.mktemp("data")
    nested_dir = test_dir / "nested"
    nested_dir.mkdir()

    file_path_1 = test_dir / "file_1.json"
    file_path_1.touch()
    with open(file_path_1, "a") as file:
        file.write(r'"[{\"user\": {\"first_name\": \"Darlene\", \"middle_name\": \"Brenda\", \"last_name\": \"Puckett\", \"zip_code\": 24065}}]"')
        #file.write(r'"[{\"date\": \"18-08-2021\", \"username\": \"gleefulTeal5\", \"address\": \"8310 Kendra Dr\", \"alias\": \"Martin Hagemann\", \"user\": {\"first_name\": \"Darlene\", \"middle_name\": \"Brenda\", \"last_name\": \"Puckett\", \"zip_code\": 24065}}, {\"username\": \"tautDoves8\", \"address\": \"6324 Ben St\", \"alias\": \"Frances Lambert\", \"id\": 229738186, \"user\": {\"first_name\": \"Lisa\", \"middle_name\": \"Robert\", \"last_name\": \"Bright\", \"zip_code\": 71241}}, {\"date\": \"17-04-2016\", \"id\": 481258423, \"address\": \"8655 Stephanie Ave\", \"username\": \"gutturalPolenta5\", \"first_name\": \"Stephen\", \"middle_name\": \"Marian\", \"last_name\": \"Do\", \"zip_code\": 10672}, {\"username\": \"cautiousHeron1\", \"id\": 644625705, \"date\": \"19-04-2016\", \"address\": \"4671 Jeremiah Ave\", \"user\": {\"info\": {\"first_name\": \"Frances\", \"middle_name\": \"Charles\", \"last_name\": \"Kulesa\", \"zip_code\": 59581}}}, {\"alias\": \"John Andrew\", \"address\": \"2012 Timothy Ave\", \"username\": \"mildEland3\", \"id\": 28849187, \"first_name\": \"Paula\", \"middle_name\": \"Marilyn\", \"last_name\": \"Fulford\", \"zip_code\": 14277}, {\"id\": 587267984, \"username\": \"dearCur2\", \"address\": \"7960 Jana Ave\", \"date\": \"14-06-2012\", \"user\": {\"info\": {\"first_name\": \"Tommye\", \"middle_name\": \"Tod\", \"last_name\": \"Keen\", \"zip_code\": 40309}}}, {\"id\": 235857399, \"alias\": \"Brooke Arnold\", \"username\": \"panickyBass4\", \"date\": \"01-10-2012\", \"first_name\": \"Leonard\", \"middle_name\": \"Wanda\", \"last_name\": \"Sanborn\", \"zip_code\": 92552}, {\"alias\": \"Elizabeth Carlton\", \"date\": \"28-06-2016\", \"id\": 292502522, \"address\": \"3020 Margaret Cir\", \"first_name\": \"Michael\", \"middle_name\": \"David\", \"last_name\": \"Demski\", \"zip_code\": 35815}, {\"id\": 409454639, \"alias\": \"Maryann Williams\", \"date\": \"30-01-2021\", \"username\": \"scornfulCaribou3\", \"first_name\": \"Robert\", \"middle_name\": \"Angela\", \"last_name\": \"Welch\", \"zip_code\": 76940}, {\"address\": \"857 Margaret Cir\", \"alias\": \"Travis Franklin\", \"id\": 813572846, \"username\": \"pleasedBittern2\", \"user\": {\"first_name\": \"Angela\", \"middle_name\": \"Paul\", \"zip_code\": 79810}}, {\"address\": \"8856 Bessie Dr\", \"date\": \"23-07-2017\", \"alias\": \"Louis Phillips\", \"username\": \"holisticUnicorn4\", \"user\": {\"info\": {\"first_name\": \"Marva\", \"middle_name\": \"Marie\", \"last_name\": \"Hubbard\", \"zip_code\": 24908}}}, {\"id\": 411439073, \"username\": \"gutturalFerret4\", \"address\": \"4311 Shirley Ave\", \"alias\": \"Ronald Raulston\", \"first_name\": \"Eric\", \"middle_name\": \"Steven\", \"last_name\": \"Enriquez\", \"zip_code\": 99173}, {\"username\": \"wingedTruffle2\", \"address\": \"9332 Anita Blvd\", \"alias\": \"Joseph Mitchell\", \"date\": \"24-12-2013\", \"user\": {\"first_name\": \"Dawn\", \"middle_name\": \"Gabriella\", \"last_name\": \"Goins\"}}, {\"address\": \"624 Tamela Cir\", \"date\": \"30-11-2015\", \"id\": 195556303, \"alias\": \"David Hernandez\", \"user\": {\"info\": {\"first_name\": \"Billy\", \"middle_name\": \"Clyde\", \"last_name\": \"Little\", \"zip_code\": 23908}}}, {\"username\": \"dearMandrill6\", \"date\": \"26-05-2014\", \"address\": \"1507 Anna Blvd\", \"alias\": \"Rebecca Mathews\", \"first_name\": \"Andy\", \"middle_name\": \"Linda\", \"last_name\": \"Hoy\"}, {\"address\": \"1803 Patricia Dr\", \"date\": \"30-01-2014\", \"id\": 993094326, \"username\": \"mildMallard6\", \"user\": {\"info\": {\"first_name\": \"Richard352\", \"middle_name\": \"Corey\", \"last_name\": \"Mcavoy\", \"zip_code\": 78964}}}, {\"username\": \"lazyMoth4\", \"address\": \"4733 Maynard Cir\", \"id\": 149808707, \"alias\": \"Michael Torres\", \"user\": {\"info\": {\"first_name\": \"Maribel\", \"middle_name\": \"Christine\", \"last_name\": \"Norman\", \"zip_code\": 83528}}}, {\"alias\": \"Robert Cremer\", \"date\": \"17-02-2016\", \"id\": 605504544, \"username\": \"lyingLapwing5\", \"user\": {\"info\": {\"first_name\": \"Patricia\", \"middle_name\": \"Diane\", \"last_name\": \"Ontiveros\", \"zip_code\": 42942}}}, {\"username\": \"mildDunbird7\", \"alias\": \"Brandi Given\", \"id\": 379443407, \"address\": \"1940 Gerald St\", \"first_name\": \"Frank\", \"middle_name\": \"Alyssa\", \"zip_code\": 14483}, {\"alias\": \"Patrick Jones\", \"address\": \"1401 Richard Dr\", \"id\": 582835066, \"date\": \"06-03-2010\", \"user\": {\"first_name\": \"Susan\", \"middle_name\": \"Richard\", \"last_name\": \"Stock\", \"zip_code\": 29300}}, {\"alias\": \"Don Noble\", \"id\": 395184929, \"date\": \"18-02-2013\", \"address\": \"135 Kathleen Cir\", \"first_name\": \"Minnie\", \"middle_name\": \"Robert\", \"last_name\": \"Hill\", \"zip_code\": 70284}, {\"username\": \"solidMandrill7\", \"date\": \"21-03-2013\", \"alias\": \"Joshua Glass\", \"address\": \"6382 Edgar Cir\", \"first_name\": \"Elvira\", \"middle_name\": \"Treva\", \"last_name\": \"Abild\", \"zip_code\": 32081}, {\"id\": 662479870, \"alias\": \"Florine Crabtree\", \"date\": \"10-08-2020\", \"address\": \"9904 Violet St\", \"user\": {\"first_name\": \"Kimberly\", \"last_name\": \"Day\"}}, {\"username\": \"dreadfulChile7\", \"date\": \"23-05-2016\", \"alias\": \"Rita Wilson\", \"address\": \"5127 Donald St\", \"user\": {\"info\": {\"middle_name\": \"Fanny\", \"last_name\": \"Mcintosh\", \"zip_code\": 39467}}}, {\"date\": \"14-03-2018\", \"address\": \"6385 Daisy St\", \"id\": 621372320, \"username\": \"humorousRhino7\"}, {\"username\": \"jumpyCoati6\", \"date\": \"26-03-2020\", \"address\": \"7784 Tracy Dr\", \"id\": 680394645}, {\"date\": \"01-11-2010\", \"address\": \"4322 Nola Blvd\", \"username\": \"euphoricSalami1\", \"id\": 902437572}, {\"username\": \"adoringKitten6\", \"date\": \"09-09-2019\", \"id\": 863112572, \"alias\": \"Juanita Hernandez\"}, {\"date\": \"20-12-2010\", \"alias\": \"Florence Shaw\", \"id\": 146219769, \"username\": \"decimalRat5\"}, {\"id\": 247683112, \"address\": \"6203 James St\", \"username\": \"adoringHare6\", \"alias\": \"Edwina Burkins\"}, {\"date\": \"30-07-2011\", \"alias\": \"Jacob Barrett\", \"username\": \"insecureHoopoe5\", \"address\": \"6475 William St\"}, {\"address\": \"7226 Lisa St\", \"id\": 2214182, \"date\": \"25-12-2010\", \"alias\": \"Tamera Gonzalez\"}, {\"alias\": \"Daniel Alonzo\", \"date\": \"24-05-2015\", \"address\": \"7548 Martha Cir\", \"id\": 938171687}, {\"id\": 368435077, \"alias\": \"Ralph Buckley\", \"username\": \"pluckyLapwing4\", \"date\": \"26-01-2018\"}, {\"username\": \"yearningBass9\", \"id\": 125118562, \"date\": \"06-12-2015\", \"address\": \"7614 Larry Dr\"}, {\"alias\": \"John Warren\", \"username\": \"mildRice3\", \"address\": \"9356 Terry Blvd\", \"id\": 990776948}, {\"id\": 608146340, \"alias\": \"Frank Buchanan\", \"address\": \"2235 George St\", \"username\": \"pacifiedFalcon9\"}, {\"address\": \"1354 Aimee Blvd\", \"username\": \"relievedBagels2\", \"date\": \"28-03-2020\", \"alias\": \"Nora Grant\"}, {\"alias\": \"Dale Broussard\", \"username\": \"pacifiedSeafowl3\", \"date\": \"01-12-2017\", \"id\": 329130337}, {\"username\": \"thriftyBaboon4\", \"id\": 444669233, \"alias\": \"Danny Skerl\", \"date\": \"07-10-2021\"}, {\"date\": \"14-11-2015\", \"alias\": \"Arthur Voss\", \"username\": \"vengefulBuck4\", \"id\": 890024587}, {\"alias\": \"Carrie Ward\", \"date\": \"12-02-2019\", \"address\": \"6386 Nancy Ave\", \"username\": \"cynicalMoth1\"}, {\"date\": \"01-01-2012\", \"username\": \"decimalPup4\", \"address\": \"3064 Donald St\", \"id\": 917003057}, {\"address\": \"9927 Thomas Dr\", \"alias\": \"Rebecca Marter\", \"id\": 786261805, \"date\": \"20-08-2019\"}, {\"id\": 583062254, \"date\": \"10-11-2021\", \"username\": \"ashamedApricots8\", \"alias\": \"Barbara Dobson\"}, {\"id\": 625338242, \"date\": \"30-04-2019\", \"alias\": \"Harold Leon\", \"username\": \"jealousChough9\"}, {\"date\": \"22-09-2012\", \"username\": \"solidBass5\", \"id\": 571192141, \"alias\": \"Ray Mora\"}, {\"date\": \"29-11-2020\", \"username\": \"somberPear5\", \"address\": \"5508 Charles Blvd\", \"id\": 102243217}, {\"address\": \"3667 Rosemary Blvd\", \"alias\": \"Howard Hoggatt\", \"username\": \"lazyRat7\", \"date\": \"10-12-2017\"}, {\"id\": 809146529, \"date\": \"18-11-2016\", \"address\": \"6489 Wesley Dr\", \"username\": \"gloomyJaguar5\"}, {\"username\": \"grizzledChowder7\", \"date\": \"26-01-2020\", \"alias\": \"Jennifer Hodge\", \"address\": \"2006 Donnell Dr\"}, {\"alias\": \"Francis Freudenburg\", \"id\": 836140075, \"date\": \"17-01-2019\", \"address\": \"3859 Joanne St\"}, {\"username\": \"relievedSeafowl1\", \"date\": \"19-06-2019\", \"address\": \"9204 Kayla Blvd\", \"alias\": \"Jerry Massey\"}, {\"date\": \"22-07-2013\", \"id\": 6753632, \"address\": \"4503 Sandra Ave\", \"username\": \"sheepishJerky1\"}, {\"id\": 239647806, \"date\": \"24-08-2013\", \"address\": \"7148 Craig Ave\", \"alias\": \"Evelyn Nunez\"}]"')

    file_2 = nested_dir / "file_2.txt"
    file_2.touch()

    return test_dir


"""
def test_determine_process_or_skip(temp_dir_with_2_json_files):
    file_path = temp_dir_with_2_json_files / 'file_1.json'
    dat = process.DataFile(file_path)

    df = pd.DataFrame({"A":[1,None,3], "B":[2,None,4]})
    res = dat.determine_process_or_skip(df)
    assert((sum(res["process"]) == 2) & (sum(res["skip"]) == 1))
"""

# TODO: Add many more unit tests

@pytest.fixture(scope="session")
def test_dir(tmp_path_factory):
    """Create a temp directory with 2 JSON files"""
    test_dir = tmp_path_factory.mktemp("output")

    return test_dir

def test_process_adds_output_file(test_dir):

    output_file_path = test_dir / "processed_data.csv"
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)

    assert os.path.exists(output_file_path)

def test_process_overwrites_output_file(test_dir):
    output_file_path = test_dir / "processed_data.csv"

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)
    first_file_time = os.path.getmtime(output_file_path)

    process.process(input_path = "tests/test_data",
                    output_file_path = output_file_path)
    second_file_time = os.path.getmtime(output_file_path)

    assert first_file_time < second_file_time

def test_process_single_record(temp_dir_json_files, test_dir):
    input_path = temp_dir_json_files
    output_file_path = test_dir / "processed_data.csv"

    process.process(input_path = input_path,
                    output_file_path = output_file_path)


#process
# less than ten total records
# no records in a file
# no records at all
# no files in dir
# more than 10 indentical results
# columns don't exist in data

# DataFile write output
# only one header

# DataFile set_records info
# number of records is the same as data
# sum(skip) + sum(process) = number of records
# "skip" and "process" are returned
# differnt indexs are used
    # data and info can be joined by index

# self.num_processed == nrow(processed_records)
# self.num_processed and self.num_skipped match records_info
