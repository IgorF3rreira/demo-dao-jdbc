package model.dao.impl;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {
	
	//A CONEXÃO SERA AQUI, ONDE TODAS AS FUNCÇÕES TERA QUE APENAS CHAMAR ELA PARA PODER TER ACESSO AO BANCO DE DADOS
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}
	

	@Override
	public void insert(Seller obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement(
					"INSERT INTO seller "
					+ "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+"VALUES "
					+"(?, ?, ?, ?, ?) ",
					//COMANDO PARA RETORNAR O ID DO ULTIMO VENDEDOR INSERIDO
					Statement.RETURN_GENERATED_KEYS
					);
			
			
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());
			
			int rowsAffected = st.executeUpdate();
			
			//VERIFICAÇÃO PARA VER SE ALGUEM FOI INSERIDO , SE O ROWS FOR MAIOR QUE 0 ENTAO INSERIU ALGUEM
			if(rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				if(rs.next()) {
				//PEGAR O VALOR DO ID GEREADO
					int id = rs.getInt(1);
					//VAMOS ATRIBUIR O ID GERADO AO OBJ PARA Q O OBJ JA ESTEJA COM O ID MAIS RECENTE NELE
					obj.setId(id);
				}
				DB.closeResultSet(rs);
				}
				
				else{
					//CASO NENHUMA LINHA SEJA AFETADA
				throw new DbException("Nenhuma linha foi afetada");				}
				}
			
			
		
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
		
		
	}

	@Override
	public void update(Seller obj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteById(Integer id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Seller findById(Integer id) {
		
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
				    "SELECT seller.*, department.Name as DepName "
				    	    + "FROM seller INNER JOIN department "
				    	    + "ON seller.DepartmentId = department.Id "
				    	    + "WHERE seller.Id = ?");
			
			st.setInt(1, id);
			rs = st.executeQuery();
			//VERIFICAÇÃO PARA SABER SE ENCONTROU UM VENDEDOR PARA ASSIM PODER TRANSFORMAR A INFOs BUSCADAS EM ORIENTADO A OBJETOS E NAO EM FORMA DE TABELA COMO O RESULTSET TRAAS
			if(rs.next()) {
				
				//PARA PODER PEGAR O ID
				Department dep = instantiateDepartment(rs);
				
				//PEGAR OS DADOS DO SELLER
				Seller obj = instantiateSeller(rs, dep);
				
				return obj;
				
				
			}
			return null;
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
			//NAO VAMOS FECHAR CONEXAO PQ ESSA PAGINA TEM OUTRAS FUNCOES QUE VAO PRECISAR DA CONEXAO ATIVA
		}
		
		
	}

	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
		Seller obj = new Seller();
		obj.setId(rs.getInt("Id"));
		obj.setName(rs.getString("Name"));
		obj.setEmail(rs.getString("Email"));
		obj.setBaseSalary(rs.getDouble("BaseSalary"));
		obj.setBirthDate(rs.getDate("BirthDate"));
		//no caso do departamento vamos querer o objeto montado q no caso é o dep que criamos
		obj.setDepartment(dep);
		return obj;
	}


	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		
		//PEGAR OS DADOS DO DEPARTMENT
		dep.setId(rs.getInt("DepartmentId"));
		dep.setName(rs.getString("DepName"));
		return dep;
	}


	@Override
	public List<Seller> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+"FROM seller INNER JOIN department "
					+"ON seller.DepartmentId = department.Id "
					+ "ORDER BY Name ");
			

			rs = st.executeQuery();
			//VERIFICAÇÃO PARA SABER SE ENCONTROU UM VENDEDOR PARA ASSIM PODER TRANSFORMAR A INFOs BUSCADAS EM ORIENTADO A OBJETOS E NAO EM FORMA DE TABELA COMO O RESULTSET TRAAS
			
			//LIST PARA ARMAZENAR OS RESULTADOS CASO HAJA MAIS DE UM 
			List<Seller> list = new ArrayList<>();
			
			//PARA NAO FICAR GERANDO UM DEP TODA VEZ Q O WHILE PASSAR , VAMOS FAZER UM MAP PARA CONTROLAR O DEP E USAR SOMENTE UMA VEZ PARA TODOS USUARIOS ENCONTRADOS NA NOSSA BUSCA
			Map<Integer,Department > map = new HashMap<>(); 
			
			while(rs.next()) {
				
				//AQUI VAI TENTAR BUSCAR NO MAP UM DEPARTAMENTO QUE TEM O ID 
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				//AQUI SE O DEP FOR IGUAL  A NULO (nao encontrar nenhum id no map) VAMOS INSTANCIAR O DEPARTAMENTO
				if(dep == null) {
					//DEPOIS DE INSTACIAR VAMOS SALVAR NO MAP PARA DEIXAR PARA PRÓXIMA VEZ VERIFICAR SE ELE JA EXISTE
					dep = instantiateDepartment(rs);
					//CÓDIGO PARA PODER SALVAR NO MAP
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				
				
				//PEGAR OS DADOS DO SELLER
				Seller obj = instantiateSeller(rs, dep);
				list.add(obj);	
				
			}
			return list;
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
			//NAO VAMOS FECHAR CONEXAO PQ ESSA PAGINA TEM OUTRAS FUNCOES QUE VAO PRECISAR DA CONEXAO ATIVA
		}
		
	}


	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+"FROM seller INNER JOIN department "
					+"ON seller.DepartmentId = department.Id "
					+"WHERE DepartmentId = ? "
					+ "ORDER BY Name ");
			
			st.setInt(1, department.getId());
			rs = st.executeQuery();
			//VERIFICAÇÃO PARA SABER SE ENCONTROU UM VENDEDOR PARA ASSIM PODER TRANSFORMAR A INFOs BUSCADAS EM ORIENTADO A OBJETOS E NAO EM FORMA DE TABELA COMO O RESULTSET TRAAS
			
			//LIST PARA ARMAZENAR OS RESULTADOS CASO HAJA MAIS DE UM 
			List<Seller> list = new ArrayList<>();
			
			//PARA NAO FICAR GERANDO UM DEP TODA VEZ Q O WHILE PASSAR , VAMOS FAZER UM MAP PARA CONTROLAR O DEP E USAR SOMENTE UMA VEZ PARA TODOS USUARIOS ENCONTRADOS NA NOSSA BUSCA
			Map<Integer,Department > map = new HashMap<>(); 
			
			while(rs.next()) {
				
				//AQUI VAI TENTAR BUSCAR NO MAP UM DEPARTAMENTO QUE TEM O ID 
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				//AQUI SE O DEP FOR IGUAL  A NULO (nao encontrar nenhum id no map) VAMOS INSTANCIAR O DEPARTAMENTO
				if(dep == null) {
					//DEPOIS DE INSTACIAR VAMOS SALVAR NO MAP PARA DEIXAR PARA PRÓXIMA VEZ VERIFICAR SE ELE JA EXISTE
					dep = instantiateDepartment(rs);
					//CÓDIGO PARA PODER SALVAR NO MAP
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				
				
				//PEGAR OS DADOS DO SELLER
				Seller obj = instantiateSeller(rs, dep);
				list.add(obj);	
				
			}
			return list;
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
			//NAO VAMOS FECHAR CONEXAO PQ ESSA PAGINA TEM OUTRAS FUNCOES QUE VAO PRECISAR DA CONEXAO ATIVA
		}
		
		
	}

	
	
}
