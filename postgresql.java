import java.sql.*;

public class Assignment2 {

	// A connection to the database.
	// This variable is kept public.
	public Connection connection;

	/*
	 * Constructor for Assignment2. Identifies the PostgreSQL driver using
	 * Class.forName() method.
	 */
	public Assignment2() {
		try {	
 			// Load JDBC driver
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Include PostgreSQL JDBC Driver in your library path!");
			e.printStackTrace();
			return;
		}
	}

	/*
	 * Using the String input parameters which are the URL, username, and
	 * password, establish the connection to be used for this object instance.
	 * If a connection already exists, it will be closed. Return true if a new
	 * connection instance was successfully established.
	 */
	public boolean connectDB(String URL, String username, String password) {
		try {
 			//Make the connection to the database, 
			connection = DriverManager.getConnection(URL, username, password);
		} catch (SQLException ex) {
			System.out.println("Connection Failed! Check output console");
			ex.printStackTrace();
			return false;
		}
		
		if (connection == null) {
			System.out.println("Failed to make connection!");
			return false;
		} 
		
		return true;
	}


	/*
	 * Closes the connection in this object instance. Returns true if the
	 * closure was successful. Returns false if this object instance previously
	 * had no active connections.
	 */
	public boolean disconnectDB() {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				System.out.println("Failed to close connection!");
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	/*
	 * Inserts a row into the student table.
	 * 
	 * dcodeis the code of the department.
	 * 
	 * Inputs and constraints must be validated.
	 * 
	 * You must check if the department exists, if the sex is one of the two
	 * values ('M' or 'F'), and if the year of study is a valid number ( > 0 &&
	 * < 6 ). Returns true if the insertion was successful, false otherwise.
	 */
	public boolean insertStudent(int sid, String lastName, String firstName,
			String sex, int age, String dcode, int yearOfStudy) {
		
		//check if sex is 'M' or 'F'
		if (!(sex.equals("M") || sex.equals("F"))) {
			return false;
		}
		
		//check if the year of study is a valid number ( > 0 && < 6 ) 
		if (!(yearOfStudy > 0 && yearOfStudy < 6)) {
			return false;
		}
		
		try {
			String sqlQ;
			ResultSet rs;
			PreparedStatement ps;
			
			//check if the department exists
			sqlQ = "SELECT * " +
	               "FROM A2.department " +
	               "WHERE dcode = ?;"; 
			ps = connection.prepareStatement(sqlQ);
			ps.setString(1,dcode);
			rs = ps.executeQuery();
			if (!rs.next()){
				//department does not exist and return false
				rs.close();
				ps.close();
				return false;
		 	} 
					
			//insert the student into the table
			sqlQ = "INSERT INTO A2.student " +
		                "VALUES (?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sqlQ);
			ps.setInt(1,sid);
			ps.setString(2, lastName);
			ps.setString(3, firstName);
			ps.setString(4, sex);
			ps.setInt(5, age);
			ps.setString(6, dcode);
			ps.setInt(7, yearOfStudy);
			ps.executeUpdate();
			//insert successfully and return true
			rs.close();
			ps.close();
			return true;
		 	
			
		} catch (SQLException e) {	
			e.printStackTrace();
		}
	
		return false;
	}

	/*
	 * Returns the number of students in department dname. Returns -1 if the
	 * department does not exist.
	 */
	public int getStudentsCount(String dname) {
		try {
			String sqlQ;
			ResultSet rs;
			PreparedStatement ps;
			
			//check if the department exists
			sqlQ = "SELECT dcode " +
	               "FROM A2.department " +
	               "WHERE dname = ?;"; 
			ps = connection.prepareStatement(sqlQ);
			ps.setString(1,dname);
			rs = ps.executeQuery();
			if (!rs.next()){
				//department does not exist and return -1
				rs.close();
				ps.close();
				return -1;
		 	} else {
		 		//get dcode of the department
		 		String dcode = rs.getString("dcode");
		 		
				//query the number of students in department dcode
				sqlQ = "SELECT count(sid) AS number " +
		               "FROM A2.student " +
					   "WHERE dcode = ?;";
				ps = connection.prepareStatement(sqlQ);
				ps.setString(1,dcode);
				rs = ps.executeQuery();
				
				//return the number of students in the department
				rs.next();
				int number = rs.getInt("number");
				rs.close();
				ps.close();
				return number;
		 	}
			
		} catch (SQLException e) {	
			e.printStackTrace();
		}
	
		return -1;
	}

	/*
	 * Returns a string with student information of student with student id sid.
	 * The output must be formatted as
	 * "firstName:lastName:sex:age:yearOfStudy:department". Returns an empty
	 * string "" if the student does not exist.
	 */
	public String getStudentInfo(int sid) {
		try {
			String sqlQ;
			ResultSet rs;
			PreparedStatement ps;
			
			//query the student whose sid is equal to the sid
			sqlQ = "SELECT * " +
	               "FROM A2.student " +
	               "WHERE sid = ?;"; 
			ps = connection.prepareStatement(sqlQ);
			ps.setInt(1, sid);
			rs = ps.executeQuery();
			
		 	if (rs.next()) {
		 		//the student with the sid exists and return the student information
		 		String studentInfo = rs.getString("sfirstname") + ":" +
		 				             rs.getString("slastname") + ":" +
		 				             rs.getString("sex") + ":" +
		 				             rs.getInt("age") + ":" +
		 				             rs.getInt("yearofstudy") + ":" +
		 				             rs.getString("dcode");
		 		rs.close();
				ps.close();
		 		return studentInfo;
		 	} else {
		 		//the student with the sid does not exist and return empty string
		 		rs.close();
				ps.close();
		 		return "";
		 	}
			
		} catch (SQLException e) {	
			e.printStackTrace();
		}
	
		return "";
	}

	/*
	 * Changes a department's name. Returns true if the change was successful,
	 * false otherwise.
	 */
	public boolean chgDept(String dcode, String newName) {
		try {
			String sqlQ;
			ResultSet rs;
			PreparedStatement ps;
			
			//check if the department exists
			sqlQ = "SELECT * " +
	               "FROM A2.department " +
	               "Where dcode = ?;"; 
			ps = connection.prepareStatement(sqlQ);
			ps.setString(1,dcode);
			rs = ps.executeQuery();
			if (!rs.next()){
				//department does not exists and return false
				rs.close();
				ps.close();
				return false;
		 	} else {
		 		
		 		//change the department's name
		 		sqlQ = "UPDATE A2.department " +
			           "SET dname = ? " +
			           "WHERE dcode = ?;"; 
				ps = connection.prepareStatement(sqlQ);
				ps.setString(1, newName);
				ps.setString(2, dcode);
				ps.executeUpdate();
				
				rs.close();
				ps.close();
				return true;
		 	}
			
		} catch (SQLException e) {	
			e.printStackTrace();
		}
//	
		return false;
	}

	/*
	 * Deletes a department. Returns true if the deletion was successful, false
	 * otherwise.
	 */
	public boolean deleteDept(String dcode) {
		try {
			String sqlQ;
			ResultSet rs;
			PreparedStatement ps;
			
			//check if the department exists
			sqlQ = "SELECT * " +
	               "FROM A2.department " +
	               "Where dcode = ?;"; 
			ps = connection.prepareStatement(sqlQ);
			ps.setString(1,dcode);
			rs = ps.executeQuery();
			if (!rs.next()){
				//the department does not exist and return false
				rs.close();
				ps.close();
				return false;
		 	} 
		 		
			//check if the delete violates foreign key constraint
			sqlQ = "SELECT * " +
		            "FROM A2.student " +
		            "Where dcode = ?;"; 
			ps = connection.prepareStatement(sqlQ);
			ps.setString(1,dcode);
			rs = ps.executeQuery();
			if (rs.next()){
				//there is student with the same dcode in student table
				rs.close();
				ps.close();
				return false;
			 } 
			
		 	//delete the department
		 	sqlQ = "DELETE FROM A2.department " +
			       "WHERE dcode = ?;"; 
			ps = connection.prepareStatement(sqlQ);
			ps.setString(1, dcode);
			ps.executeUpdate();
				
			rs.close();
			ps.close();
			return true;
		 	
			
		} catch (SQLException e) {	
			e.printStackTrace();
		}
//	
		return false;
	}

	/*
	 * Returns a string with all the courses a student with student id sid has
	 * taken. Each course will be in a separate line.
	 * 
	 * Eg. "courseName1:department:semester:year:grade
	 * courseName2:department:semester:year:grade"
	 * 
	 * Returns an empty string "" if the student does not exist.
	 */
	public String listCourses(int sid) {
		
		try {
			String sqlQ;
			ResultSet rs;
			PreparedStatement ps;
			String listCourses = "";
			
			//check if the student exists
			sqlQ = "SELECT cname, dname, semester, year, grade " +
	               "FROM A2.studentCourse " +
	               " NATURAL JOIN A2.courseSection " +
	               " NATURAL JOIN A2.course " +
	               " NATURAL JOIN A2.department " +
	               "WHERE sid = ?;"; 
			ps = connection.prepareStatement(sqlQ);
			ps.setInt(1,sid);
			rs = ps.executeQuery();
			if (rs.next()){
				do {
					listCourses += rs.getString("cname").trim() + ":" +
				             rs.getString("dname").trim() + ":" +
				             rs.getInt("semester") + ":" +
				             rs.getInt("year") + ":" +
				             rs.getInt("grade");
				} while (rs.next());
				return listCourses;
		 	} else {
		 		//student does not exist and return an empty string
				rs.close();
				ps.close();
				return "";
		 	}
			
			
		} catch (SQLException e) {	
			e.printStackTrace();
		}
	
		return "";

	}

	/*
	 * Increases the grades of all the students who took a course in the course
	 * section identified by csid by 10%. Returns true if the update was
	 * successful, false otherwise. Do not not allow grades to go over 100%.
	 */
	public boolean updateGrades(int csid) {
		
		try {
			String sqlQ;
			ResultSet rs;
			PreparedStatement ps;
			
			sqlQ = "SELECT * " +
		           "FROM A2.studentCourse " +
		           "WHERE csid = ?;"; 
			ps = connection.prepareStatement(sqlQ);
			ps.setInt(1,csid);
			rs = ps.executeQuery();
			if (!rs.next()) {
				//course section are not taken by any students
				rs.close();
				ps.close();
				return false;
			}
			
			//update the course section grade
			sqlQ = "UPDATE A2.studentCourse " +
	               "SET grade = least(grade * 1.1, 100) " +
	               "WHERE csid = ?;"; 
			ps = connection.prepareStatement(sqlQ);
			ps.setInt(1,csid);
			ps.executeUpdate();
			
			rs.close();
			ps.close();
			return true;

			
		} catch (SQLException e) {	
			e.printStackTrace();
		}
	
		return false;
	}

	/*
	 * Create a table containing all the female students in "Computer Science"
	 * department who are in their fourth year of study.
	 * 
	 * The name of the table is femaleStudents and the attributes are:
	 * - sid INTEGER (student id)
	 * - fname CHAR (20) (first name)
	 * - lname CHAR (20) (last name)
	 * 
	 * Returns false and does not nothing if the table already exists. Returns
	 * true if the database was successfully created, false otherwise.
	 */
	public boolean updateDB() {
		try {
			String sqlQ;
			ResultSet rs;
			Statement sql;
			sql = connection.createStatement();
			
			//check if the table already exists
			sqlQ = "SELECT 1 " +
		           "FROM information_schema.tables " +
		           "WHERE table_schema = 'a2' " +
		           " AND table_name = 'femalestudents';"; 
			rs = sql.executeQuery(sqlQ);
			//rs = connection.getMetaData().getTables(null, "A2", "FEMALESTUDENTS", new String[] {"TABLE"});
			if (rs.next()) {
				//table already exists and return false
				sql.close();
				rs.close();
				return false;
			}
			
			//create the table
			sqlQ = "CREATE TABLE a2.femaleStudents " +
				   "( " +
	               "sid INTEGER, " +
	               "fname CHAR(20), " +
	               "lname CHAR(20) " +
	               ");"; 
			sql.executeUpdate(sqlQ);
			
			//insert values
			sqlQ = "INSERT INTO a2.femaleStudents " +
					"( " +
		            "SELECT sid, sfirstname AS fname, slastname AS lname " +
		            "FROM A2.student " +
		            " NATURAL JOIN A2.department " +
		            "WHERE sex = 'F' " +
		            " AND dname = 'Computer Science' " +
		            " AND yearofstudy = 4 " +
		            ");"; 
			int line = sql.executeUpdate(sqlQ);
			
			sql.close();
			rs.close();

			if (line == 0) {
				return false;
			}
			return true;

			
		} catch (SQLException e) {	
			e.printStackTrace();
		}
	
		return false;
	}

	/*
	public void insertRandomDep() {
		
		try {
			String sqlQ;
			Statement sql;
			sql = connection.createStatement();
			sqlQ = "INSERT INTO A2.department VALUES " +
		           "('CSC', 'Computer Science');";
			System.out.println("Executing this command: \n" + sqlQ.replaceAll("\\s+", " ") + "\n");
			sql.executeUpdate(sqlQ);
				
			sqlQ = "INSERT INTO A2.department VALUES " +
			       "('MGT', 'Managemenet');"; 
			System.out.println("Executing this command: \n" + sqlQ.replaceAll("\\s+", " ") + "\n");
			sql.executeUpdate(sqlQ);
				
			sqlQ = "INSERT INTO A2.department VALUES " +
			           "('VPM', 'Music');"; 
			System.out.println("Executing this command: \n" + sqlQ.replaceAll("\\s+", " ") + "\n");
			sql.executeUpdate(sqlQ);
			
			sqlQ = "INSERT INTO A2.department VALUES " +
			           "('STA', 'Statistic');"; 
			System.out.println("Executing this command: \n" + sqlQ.replaceAll("\\s+", " ") + "\n");
			sql.executeUpdate(sqlQ);
				
			sql.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	
	}
	
		public void insertRandomCourse() {
			
			try {
				String sqlQ;
				Statement sql;
				sql = connection.createStatement();
				
				sqlQ = "INSERT INTO A2.course VALUES " +
				           "(0, 'CSC', 'Computer');"; 
				System.out.println("Executing this command: \n" + sqlQ.replaceAll("\\s+", " ") + "\n");
				sql.executeUpdate(sqlQ);
				
				sqlQ = "INSERT INTO A2.courseSection VALUES " +
				           "(0, 0, 'CSC', 2019, 1, 1, null);"; 
				System.out.println("Executing this command: \n" + sqlQ.replaceAll("\\s+", " ") + "\n");
				sql.executeUpdate(sqlQ);
				
				sqlQ = "INSERT INTO A2.studentCourse VALUES " +
				           "(0, 0, 90);"; 
				System.out.println("Executing this command: \n" + sqlQ.replaceAll("\\s+", " ") + "\n");
				sql.executeUpdate(sqlQ);
				
				sqlQ = "INSERT INTO A2.studentCourse VALUES " +
				           "(1, 0, 99);"; 
				System.out.println("Executing this command: \n" + sqlQ.replaceAll("\\s+", " ") + "\n");
				sql.executeUpdate(sqlQ);
					
				sql.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		
		}
	
	public static void main(String[] args) {
		Assignment2 a2 = new Assignment2();
		a2.connectDB("jdbc:postgresql://localhost:5432/", "lujiayu4", "password");
		
		a2.insertRandomDep();
		
		boolean insert1 = a2.insertStudent(0, "Black", "Smith", "F", 20, "CSC", 4);
		boolean insert2 = a2.insertStudent(1, "Josiah", "Alex", "M", 19, "CSC", 2);
		boolean insert3 = a2.insertStudent(2, "Alice", "Wang", "F", 18, "MGT", 1);
		boolean insert4 = a2.insertStudent(3, "Kim", "Maven", "F", 18, "VPM", 1);
		boolean insert5 = a2.insertStudent(4, "Ana", "Benlov", "T", 18, "VPM", 1);
		boolean insert6 = a2.insertStudent(5, "Java", "Github", "F", 18, "CHE", 1);
		a2.insertStudent(5, "wow", "good", "F", 18, "CSC", 4);
		System.out.println("valid insert1:" + insert1);
		System.out.println("valid insert2:" + insert2);
		System.out.println("valid insert3:" + insert3);
		System.out.println("valid insert4:" + insert4);
		System.out.println("invalid insert5:" + insert5);
		System.out.println("invalid insert6:" + insert6);
		
		a2.insertRandomCourse();
		
		int num1 = a2.getStudentsCount("Computer Science");
		int num2 = a2.getStudentsCount("Music");
		int num3 = a2.getStudentsCount("Statistic");
		int num4 = a2.getStudentsCount("Math");
		System.out.println("Should be 2:" + num1);
		System.out.println("Should be 1:" + num2);
		System.out.println("Should be 0:" + num3);
		System.out.println("Should be -1:" + num4);
		
		String studentInfo1 = a2.getStudentInfo(0);
		String studentInfo2 = a2.getStudentInfo(1);
		String studentInfo3 = a2.getStudentInfo(10);
		System.out.println("Student exist:" + studentInfo1);
		System.out.println("Student exist:" + studentInfo2);
		System.out.println("Student does not exist:" + studentInfo3);
		
		boolean p1 = a2.chgDept("VPM", "Elegent Music");
		boolean p2 = a2.chgDept("CHE", "Chemistry");
		System.out.println("change exist dep(true):" + p1);
		System.out.println("change non-exist dep(false):" + p2);
		
		boolean d1 = a2.deleteDept("STA");
		boolean d2 = a2.deleteDept("VPM");
		System.out.println("delete exist dep(true):" + d1);
		System.out.println("delete constrain dep(false):" + d2);
		
		
		String lCourse1 = a2.listCourses(0);
		String lCourse2 = a2.listCourses(10);
		System.out.println("Student exist:" + lCourse1);
		System.out.println("Student not exist:" + lCourse2);
		
		boolean u1 = a2.updateGrades(0);
		boolean u2 = a2.updateGrades(10);
		System.out.println("course exist(true):" + u1);
		System.out.println("course not exist(false):" + u2);
		
		boolean up1 = a2.updateDB();
		System.out.println("true:" + up1);
		
		a2.disconnectDB();
	}
	*/

}

